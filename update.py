import json
import csv
import time
import requests
import os
import sys
import datetime
import argparse

# --- CONFIGURATION ---
REGIONS_FILE = "regions.json"
WIKIDATA_URL = "https://query.wikidata.org/sparql"
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OSM_DIR = "osm"             
DATA_DIR = "data_overpass"  
METADATA_FILE = "metadata.json"

def fetch_osm_with_retry(area_id, retries=3):
    query = f"""
    [out:json][timeout:600];
    area({area_id})->.searchArea;
    (
      node["wikidata"](area.searchArea);
      way["wikidata"](area.searchArea);
      relation["wikidata"](area.searchArea);
    );
    out tags qt;
    """
    for attempt in range(retries):
        try:
            response = requests.get(OVERPASS_URL, params={'data': query}, timeout=605)
            response.raise_for_status()
            data = response.json()
            if 'elements' in data: return data
        except Exception as e:
            print(f"      [!] Attempt {attempt+1}/{retries} failed: {e}")
            time.sleep(5)
    return None

def get_wikidata_clean(qid, region_name):
    query = f"""SELECT DISTINCT ?qid ?lat ?lon ?label WHERE {{
       ?item wdt:P131* wd:{qid}; wdt:P625 ?loc .
       FILTER NOT EXISTS {{ ?item wdt:P582 ?end. FILTER(?end < NOW()) }}
       FILTER NOT EXISTS {{ ?item wdt:P576 ?dissolved. FILTER(?dissolved < NOW()) }}
       MINUS {{ ?item p:P131 ?stmt . ?stmt pq:P582 ?linkEnd . FILTER(?linkEnd < NOW()) }}
       BIND(STRAFTER(STR(?item), '/entity/') as ?qid) 
       BIND(geof:latitude(?loc) as ?lat) 
       BIND(geof:longitude(?loc) as ?lon) 
       OPTIONAL {{ ?item rdfs:label ?label. FILTER(lang(?label)='en') }} 
       OPTIONAL {{ ?item rdfs:label ?label. FILTER(lang(?label)='it') }}
    }}"""
    
    print(f"   -> Downloading Wikidata for {region_name} ({qid})...", end=" ", flush=True)
    try:
        headers = {'User-Agent': 'ItaliaWikidataCheck/1.0', 'Accept': 'text/csv'}
        r = requests.get(WIKIDATA_URL, params={'query': query}, headers=headers)
        r.raise_for_status()
        print("OK.")
        return r.text
    except Exception as e:
        print(f"ERROR: {e}")
        return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--region", default="all", help="Specific region key or 'all'")
    args = parser.parse_args()

    if not os.path.exists(REGIONS_FILE): return
    if not os.path.exists(OSM_DIR): os.makedirs(OSM_DIR)
    if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)

    with open(REGIONS_FILE, 'r', encoding='utf-8') as f:
        regions = json.load(f)

    # Load existing metadata to preserve dates of skipped regions
    region_meta = {}
    if os.path.exists(METADATA_FILE):
        try:
            with open(METADATA_FILE, 'r') as f:
                old_meta = json.load(f)
                region_meta = old_meta.get("regions", {})
        except: pass

    target_regions = regions.keys() if args.region == 'all' else [args.region]
    print(f"--- Starting Update Process (Target: {args.region}) ---")
    
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M UTC")
    processed_count = 0

    for key in target_regions:
        if key not in regions: 
            print(f"Skipping unknown region: {key}")
            continue

        config = regions[key]
        print(f"\n--- Processing {config['name']} ---")
        
        file_osm = os.path.join(OSM_DIR, f"osm_{key}.json")
        osm_data = None
        current_osm_date = "Unknown"
        is_cached = False
        
        # 1. Overpass Download
        print(f"   -> Fetching OSM items...", end=" ")
        sys.stdout.flush()
        new_data = fetch_osm_with_retry(config['osm'])
        
        if new_data:
            print("Success.")
            with open(file_osm, 'w', encoding='utf-8') as f:
                json.dump(new_data, f)
            osm_data = new_data
            current_osm_date = now_str
        else:
            print("Failed. Checking cache...")
            is_cached = True

        # 2. Cache Fallback
        if not osm_data and os.path.exists(file_osm):
            try:
                ts = os.path.getmtime(file_osm)
                file_date = datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M UTC")
                with open(file_osm, 'r', encoding='utf-8') as f:
                    cached = json.load(f)
                    if 'elements' in cached:
                        osm_data = cached
                        current_osm_date = f"{file_date} (Cached)"
                        print(f"   -> Using valid cache from {file_date}")
            except Exception as e:
                print(f"   [!] Cache corrupt: {e}")
        
        if not osm_data:
            print("   [CRITICAL] No Data. Skipping.")
            continue

        # Metadata update for this region
        region_meta[key] = { "osm": current_osm_date, "wiki": now_str }

        # Build IDs
        osm_ids = {}
        for el in osm_data.get('elements', []):
            if 'wikidata' in el.get('tags', {}):
                raw_tags = el['tags']['wikidata'].replace(',', ';')
                for raw in raw_tags.split(';'):
                    raw = raw.strip().upper()
                    if raw.startswith('Q'): osm_ids[raw] = f"{el['type']}/{el['id']}"

        # Wikidata
        csv_text = get_wikidata_clean(config['qid'], config['name'])
        if not csv_text: continue

        features = []
        seen = set()
        reader = csv.DictReader(csv_text.splitlines())
        
        for row in reader:
            qid = row.get('qid') or row.get('?qid')
            if not qid: continue
            qid = qid.split('/')[-1].upper()
            if qid in seen: continue
            
            try:
                lat = float(row.get('lat') or row.get('?lat'))
                lon = float(row.get('lon') or row.get('?lon'))
            except: continue

            label = row.get('label') or row.get('?label') or qid
            status = "done" if osm_ids.get(qid) else "missing"
            
            features.append({
                "type": "Feature",
                "properties": { "wikidata": qid, "name": label, "status": status, "osm_id": osm_ids.get(qid) },
                "geometry": { "type": "Point", "coordinates": [lon, lat] }
            })
            seen.add(qid)

        with open(os.path.join(DATA_DIR, f"data_{key}.geojson"), 'w', encoding='utf-8') as f:
            json.dump({"type": "FeatureCollection", "features": features}, f)
        
        print(f"   -> Saved {len(features)} items")
        processed_count += 1
        time.sleep(2)

    # Save Metadata
    if processed_count > 0:
        with open(METADATA_FILE, 'w') as f:
            # Global dates are just "Now" because we touched the file
            json.dump({ "global_osm_date": now_str, "global_wiki_date": now_str, "regions": region_meta }, f)

    print("\nDONE.")

if __name__ == "__main__":
    main()
