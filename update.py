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
DATA_DIR = "data" 

def fetch_osm_overpass(area_id):
    """
    Fetches all nodes, ways, and relations with a 'wikidata' tag 
    within a specific OSM area ID. Returns the JSON result or None.
    """
    # Overpass query: Get elements with 'wikidata' tag in the area
    # We only need tags and IDs, 'out tags' is sufficient and faster.
    query = f"""
    [out:json][timeout:180];
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
        print(f"      [!] Overpass download failed: {e}")
        return None

def get_wikidata_clean(qid, region_name):
    """
    Fetches Wikidata items for the region, excluding historical items
    and filtering out broken P131 links (Farm Fix).
    """
    query = f"""SELECT DISTINCT ?qid ?lat ?lon ?label WHERE {{
       # 1. Base search
       ?item wdt:P131* wd:{qid}; wdt:P625 ?loc .

       # 2. Exclude historical items
       FILTER NOT EXISTS {{ ?item wdt:P582 ?end. FILTER(?end < NOW()) }}
       FILTER NOT EXISTS {{ ?item wdt:P576 ?dissolved. FILTER(?dissolved < NOW()) }}
       
       # 3. FIX: Exclude items with ANY expired location link (P131)
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
    if not os.path.exists(REGIONS_FILE):
        print("Regions file not found.")
        return
    
    # Ensure directories exist
    if not os.path.exists(OSM_DIR): os.makedirs(OSM_DIR)
    if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)

    with open(REGIONS_FILE, 'r', encoding='utf-8') as f:
        regions = json.load(f)

    print(f"--- Starting Update Process ---")
    processed = 0
    # Current time for metadata
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M UTC")

    for key, config in regions.items():
        print(f"\n--- Processing {config['name']} ---")
        
        file_osm = os.path.join(OSM_DIR, f"osm_{key}.json")
        osm_data = None
        
        # 1. Try to fetch fresh OSM data
        if "osm" in config:
            print(f"   -> Fetching OSM data (Area {config['osm']})...", end=" ")
            sys.stdout.flush()
            new_data = fetch_osm_overpass(config['osm'])
            
            if new_data:
                print("Success. Updating local file.")
                with open(file_osm, 'w', encoding='utf-8') as f:
                    json.dump(new_data, f)
                osm_data = new_data
            else:
                print("Failed. Using cached file if available.")
        
        # 2. Load OSM data (fresh or cached)
        if not osm_data and os.path.exists(file_osm):
            try:
                with open(file_osm, 'r', encoding='utf-8') as f:
                    osm_data = json.load(f)
            except Exception as e:
                print(f"   [!] Error reading cached OSM file: {e}")
        
        if not osm_data:
            print(f"   [!] Skipping: No OSM data available.")
            continue

        # Build OSM ID map
        osm_ids = {}
        for el in osm_data.get('elements', []):
            tags = el.get('tags', {})
            if 'wikidata' in tags:
                # Handle multiple QIDs (split by semicolon), simplify to uppercase
                raw_ids = tags['wikidata'].split(';')
                for raw in raw_ids:
                    raw = raw.strip().upper()
                    if raw.startswith('Q'):
                        # Map QID to OSM Object (type/id)
                        osm_ids[raw] = f"{el['type']}/{el['id']}"

        # 3. Fetch Wikidata
        csv_text = get_wikidata_clean(config['qid'], config['name'])
        if not csv_text: continue

        # 4. Compare and Build Features
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
                label = row.get('label') or row.get('?label') or qid
            except: continue

            osm_ref = osm_ids.get(qid)
            status = "missing"
            if osm_ref: status = "done"

            features.append({
                "type": "Feature",
                "properties": { 
                    "wikidata": qid, 
                    "name": label, 
                    "status": status, 
                    "osm_id": osm_ref 
                },
                "geometry": { "type": "Point", "coordinates": [lon, lat] }
            })
            seen.add(qid)

        # 5. Save Output
        outfile = os.path.join(DATA_DIR, f"data_{key}.geojson")
        with open(outfile, 'w', encoding='utf-8') as f:
            json.dump({"type": "FeatureCollection", "features": features}, f)
        
        print(f"   -> Saved {len(features)} items to {outfile}")
        processed += 1
        
        # Sleep to be gentle on APIs
        time.sleep(5)

    # Write Metadata
    if processed > 0:
        with open("metadata.json", "w") as f:
            json.dump({ 
                "osm_date": now_str, 
                "wiki_date": now_str, 
                "regions_count": len(regions) 
            }, f)

    print(f"\nDONE. Processed {processed} regions.")

if __name__ == "__main__":
    main()
