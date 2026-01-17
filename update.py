import json
import csv
import time
import requests
import os
import sys
import datetime

# --- CONFIGURATION ---
REGIONS_FILE = "regions.json"
WIKIDATA_URL = "https://query.wikidata.org/sparql"
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OSM_DIR = "osm"             
DATA_DIR = "data_overpass"  

def fetch_osm_overpass(area_id):
    # Timeout raised to 240s
    query = f"""
    [out:json][timeout:240];
    area({area_id})->.searchArea;
    (
      node["wikidata"](area.searchArea);
      way["wikidata"](area.searchArea);
      relation["wikidata"](area.searchArea);
    );
    out tags qt;
    """
    try:
        response = requests.get(OVERPASS_URL, params={'data': query})
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"      [!] Overpass failed: {e}")
        return None

def get_wikidata_clean(qid, region_name):
    query = f"""SELECT DISTINCT ?qid ?lat ?lon ?label WHERE {{
       ?item wdt:P131* wd:{qid}; wdt:P625 ?loc .
       FILTER NOT EXISTS {{ ?item wdt:P582 ?end. FILTER(?end < NOW()) }}
       FILTER NOT EXISTS {{ ?item wdt:P576 ?dissolved. FILTER(?dissolved < NOW()) }}
       MINUS {{ 
           ?item p:P131 ?stmt . 
           ?stmt pq:P582 ?linkEnd . 
           FILTER(?linkEnd < NOW()) 
       }}
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
    if not os.path.exists(REGIONS_FILE): return
    if not os.path.exists(OSM_DIR): os.makedirs(OSM_DIR)
    if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)

    with open(REGIONS_FILE, 'r', encoding='utf-8') as f:
        regions = json.load(f)

    print(f"--- Starting Update Process ---")
    processed = 0
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M UTC")
    
    region_meta = {}
    all_osm_dates = set()

    for key, config in regions.items():
        print(f"\n--- Processing {config['name']} ---")
        
        file_osm = os.path.join(OSM_DIR, f"osm_{key}.json")
        osm_data = None
        current_osm_date = "Unknown"
        
        # 1. Update OSM Data (Overpass)
        if "osm" in config:
            print(f"   -> Fetching OSM items...", end=" ")
            sys.stdout.flush()
            new_data = fetch_osm_overpass(config['osm'])
            if new_data:
                print("Success.")
                with open(file_osm, 'w', encoding='utf-8') as f:
                    json.dump(new_data, f)
                osm_data = new_data
                current_osm_date = now_str
            else:
                print("Failed. Using cache.")
        
        # 2. Load Cache
        if not osm_data and os.path.exists(file_osm):
            try:
                # Use file date if update failed
                ts = os.path.getmtime(file_osm)
                current_osm_date = datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M UTC")
                with open(file_osm, 'r', encoding='utf-8') as f:
                    osm_data = json.load(f)
            except Exception as e:
                print(f"   [!] Cache error: {e}")
        
        if not osm_data: 
            print("   [!] No OSM data available. Skipping.")
            continue

        region_meta[key] = { "osm": current_osm_date, "wiki": now_str }
        all_osm_dates.add(current_osm_date)

        # Build IDs
        osm_ids = {}
        for el in osm_data.get('elements', []):
            if 'wikidata' in el.get('tags', {}):
                # Robust split by semicolon or comma
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
            
            lat = row.get('lat') or row.get('?lat')
            lon = row.get('lon') or row.get('?lon')
            if not lat or not lon: continue

            try:
                lat = float(lat)
                lon = float(lon)
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
        processed += 1
        time.sleep(2)

    # Metadata
    global_osm_date = now_str
    if len(all_osm_dates) > 1:
        global_osm_date = sorted(list(all_osm_dates))[-1] + " *"
    elif len(all_osm_dates) == 1:
        global_osm_date = list(all_osm_dates)[0]

    if processed > 0:
        with open("metadata.json", "w") as f:
            json.dump({ "global_osm_date": global_osm_date, "global_wiki_date": now_str, "regions": region_meta }, f)

    print("\nDONE.")

if __name__ == "__main__":
    main()
