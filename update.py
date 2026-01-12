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
OSM_DIR = "osm"   # Input Folder
DATA_DIR = "data" # Output Folder

# Classes considered "Broad" (Orange) regardless of location
# Mountains (Q46831), Traffic routes (Q205466), Passes (Q15312), Bodies of water (Q15324)
BROAD_CLASSES = ['Q46831', 'Q205466', 'Q15312', 'Q15324', 'Q4022'] 

def get_wikidata_smart(qid, region_name):
    query = f"""SELECT ?qid ?lat ?lon ?label ?class (COUNT(?parent) AS ?p131count) WHERE {{
       # 1. Main Search
       ?item wdt:P131* wd:{qid}; wdt:P625 ?loc .
       
       # 2. Get Class (for Orange logic)
       OPTIONAL {{ ?item wdt:P31 ?classItem . BIND(STRAFTER(STR(?classItem), '/entity/') as ?class) }}

       # 3. COUNT direct P131 connections (to detect cross-border items)
       OPTIONAL {{ ?item wdt:P131 ?parent }}

       # 4. EXCLUDE Historical Items (Ended or Dissolved)
       FILTER NOT EXISTS {{ ?item wdt:P582 ?end. FILTER(?end < NOW()) }}
       FILTER NOT EXISTS {{ ?item wdt:P576 ?dissolved. FILTER(?dissolved < NOW()) }}
       
       # 5. EXCLUDE Items where the link to the region is historical (The Farm Fix!)
       FILTER NOT EXISTS {{ 
           ?item p:P131 ?stmt . 
           ?stmt ps:P131 wd:{qid} .
           ?stmt pq:P582 ?linkEnd . 
           FILTER(?linkEnd < NOW()) 
       }}

       BIND(STRAFTER(STR(?item), '/entity/') as ?qid) 
       BIND(geof:latitude(?loc) as ?lat) 
       BIND(geof:longitude(?loc) as ?lon) 
       
       OPTIONAL {{ ?item rdfs:label ?label. FILTER(lang(?label)='en') }} 
       OPTIONAL {{ ?item rdfs:label ?label. FILTER(lang(?label)='it') }}
    }}
    GROUP BY ?qid ?lat ?lon ?label ?class
    """
    
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
        print(f"ERROR: {REGIONS_FILE} missing.")
        return

    # Create output dir if missing
    if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)

    with open(REGIONS_FILE, 'r', encoding='utf-8') as f:
        regions = json.load(f)

    print(f"--- Starting Online Update (OSM: {OSM_DIR} -> DATA: {DATA_DIR}) ---")
    
    processed_count = 0
    # On GitHub Actions, file dates are reset, so we use current time for OSM date too
    current_time_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M UTC")

    for key, config in regions.items():
        file_osm = os.path.join(OSM_DIR, f"osm_{key}.json")
        
        # Check if OSM file exists in the osm/ folder
        if not os.path.exists(file_osm):
            continue

        print(f"\n--- Processing {config['name']} ---")

        # 1. Load OSM Data
        osm_ids = {}
        try:
            with open(file_osm, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for element in data.get('elements', []):
                    tags = element.get('tags', {})
                    if 'wikidata' in tags:
                        raw = tags['wikidata'].split(';')[0].strip().upper()
                        if raw.startswith('Q'):
                            osm_ids[raw] = f"{element['type']}/{element['id']}"
        except Exception as e:
            print(f"   OSM Error: {e}")
            continue

        # 2. Load Wikidata Live
        csv_text = get_wikidata_smart(config['qid'], config['name'])
        if not csv_text: continue

        # 3. Process Features
        features = []
        processed_qids = set()
        
        reader = csv.DictReader(csv_text.splitlines())
        
        for row in reader:
            qid = row.get('qid') or row.get('?qid')
            if not qid: continue
            qid = qid.split('/')[-1].upper()
            if qid in processed_qids: continue

            try:
                lat = float(row.get('lat') or row.get('?lat'))
                lon = float(row.get('lon') or row.get('?lon'))
                label = row.get('label') or row.get('?label') or qid
                item_class = row.get('class') or row.get('?class')
                p131_count = int(row.get('p131count') or row.get('?p131count') or 1)
            except: continue

            osm_ref = osm_ids.get(qid)
            status = "missing"
            if osm_ref: status = "done"
            
            # ORANGE LOGIC:
            # 1. Is it a "Broad Class" (Mountain, etc)? -> Orange
            # 2. Does it have MORE THAN 1 administrative parent? -> Orange (likely border/cross-region)
            if item_class in BROAD_CLASSES or p131_count > 1:
                status = "broad"

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
            processed_qids.add(qid)

        # 4. Save to data/ folder
        outfile = os.path.join(DATA_DIR, f"data_{key}.geojson")
        with open(outfile, 'w', encoding='utf-8') as f:
            json.dump({"type": "FeatureCollection", "features": features}, f)
        
        print(f"   -> Saved {len(features)} items to {outfile}")
        processed_count += 1
        time.sleep(1)

    # Metadata
    if processed_count > 0:
        with open("metadata.json", "w") as f:
            json.dump({ 
                "osm_date": current_time_str, 
                "wiki_date": current_time_str, 
                "regions_count": len(regions) 
            }, f)

    print(f"\nDONE.")

if __name__ == "__main__":
    main()
