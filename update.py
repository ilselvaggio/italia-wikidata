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
    # Timeout 600s
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
    parser.add_argument("--region", default="all", help="Region key")
    args = parser.parse_args()

    if not os.path.exists(REGIONS_FILE): return
    if not os.path.exists(OSM_DIR): os.makedirs(OSM_DIR)
    if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)

    with open(REGIONS_FILE, 'r', encoding='utf-8') as f:
        regions = json.load(f)

    # ALTE METADATA LADEN (WICHTIG FÜR CACHE DATUM)
    old_region_meta = {}
    if os.path.exists(METADATA_FILE):
        try:
            with open(METADATA_FILE, 'r') as f:
                full_meta = json.load(f)
                old_region_meta = full_meta.get("regions", {})
        except: pass

    target_regions = regions.keys() if args.region == 'all' else [args.region]
    print(f"--- Starting Update Process (Target: {args.region}) ---")
    
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M UTC")
    
    # Wir kopieren erst mal alles Alte in das Neue, damit nicht bearbeitete Regionen bleiben
    new_region_meta = old_region_meta.copy()
    
    processed_count = 0

    for key in target_regions:
        if key not in regions: continue
        config = regions[key]
        print(f"\n--- Processing {config['name']} ---")
        
        file_osm = os.path.join(OSM_DIR, f"osm_{key}.json")
        osm_data = None
        current_osm_date = "Unknown"
        
        # 1. Download
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
            print("Failed. Using cache.")
            
            # CACHE LOGIK: Datum aus alter Metadata holen!
            if key in old_region_meta:
                old_date = old_region_meta[key].get("osm", "Unknown")
                # Sicherstellen, dass "(Cached)" dran steht
                if "(Cached)" not in old_date:
                    current_osm_date = f"{old_date} (Cached)"
                else:
                    current_osm_date = old_date
            else:
                current_osm_date = "Unknown (Cached)"
                
            # Datei laden
            if os.path.exists(file_osm):
                try:
                    with open(file_osm, 'r', encoding='utf-8') as f:
                        osm_data = json.load(f)
                except: pass

        if not osm_data:
            print("   [CRITICAL] No Data. Skipping.")
            continue

        new_region_meta[key] = { "osm": current_osm_date, "wiki": now_str }

        # IDs
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

    # Metadata Globales Datum
    global_osm_date = now_str
    if args.region == 'all':
        # Wenn alles lief, ist "Now" okay, es sei denn es gab Cached
        # Wir prüfen, ob irgendwas cached ist
        pass 
    
    with open(METADATA_FILE, 'w') as f:
        json.dump({ "global_osm_date": now_str, "global_wiki_date": now_str, "regions": new_region_meta }, f)

    print("\nDONE.")

if __name__ == "__main__":
    main()
