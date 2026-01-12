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
OSM_DIR = "osm"   
DATA_DIR = "data" 

# Classes that are considered "Broad" (Orange status)
# Q46831 (Mountain range), Q205466 (Traffic route), Q15312 (Mountain pass)
# Added: Q355304 (Watercourse), Q165 (Sea/Ocean)
BROAD_CLASSES = ['Q46831', 'Q205466', 'Q15312', 'Q355304', 'Q165'] 

def get_wikidata_smart(qid, region_name):
    # Query logic:
    # 1. Get items in region (P131)
    # 2. Filter out historical items (End time P582 or Dissolved P576)
    # 3. Filter out items where the LINK to the region is historical (P131 qualifier P582)
    
    query = f"""SELECT ?qid ?lat ?lon ?label ?class WHERE {{
       ?item wdt:P131* wd:{qid}; wdt:P625 ?loc .
       
       # Get Class (P31) for Orange logic
       OPTIONAL {{ ?item wdt:P31 ?classItem . BIND(STRAFTER(STR(?classItem), '/entity/') as ?class) }}

       # FILTER: Exclude historical objects (ended or dissolved)
       FILTER NOT EXISTS {{ ?item wdt:P582 ?end. FILTER(?end < NOW()) }}
       FILTER NOT EXISTS {{ ?item wdt:P576 ?dissolved. FILTER(?dissolved < NOW()) }}
       
       # FILTER: Exclude objects where the P131 connection itself is historical (e.g. farm moved border)
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

def get_file_date(filepath):
    try:
        timestamp = os.path.getmtime(filepath)
        return datetime.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M")
    except:
        return "Unknown"

def main():
    if not os.path.exists(REGIONS_FILE):
        print(f"ERROR: {REGIONS_FILE} missing.")
        return

    if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)

    with open(REGIONS_FILE, 'r', encoding='utf-8') as f:
        regions = json.load(f)

    print(f"--- Starting Smart Update (OSM: {OSM_DIR} -> DATA: {DATA_DIR}) ---")
    
    processed_count = 0
    latest_osm_date = "2000-01-01 00:00"

    for key, config in regions.items():
        file_osm = os.path.join(OSM_DIR, f"osm_{key}.json")
        
        if not os.path.exists(file_osm):
            # print(f"Skipping {key} (no OSM file found)")
            continue

        # Track the latest OSM file date for metadata
        f_date = get_file_date(file_osm)
        if f_date > latest_osm_date:
            latest_osm_date = f_date

        print(f"\n--- Processing {config['name']} ---")

        # 1. Load OSM
        osm_ids = {}
        try:
            with open(file_osm, 'r', encoding='utf-8') as f:
                data = json.load(f)
                elements = data.get('elements', [])
                for element in elements:
                    tags = element.get('tags', {})
                    el_type = element.get('type')
                    el_id = element.get('id')
                    if 'wikidata' in tags:
                        raw = tags['wikidata'].split(';')[0].strip().upper()
                        if raw.startswith('Q'):
                            osm_ids[raw] = f"{el_type}/{el_id}"
        except Exception as e:
            print(f"   OSM READ ERROR: {e}")
            continue

        # 2. Load Wikidata
        csv_text = get_wikidata_smart(config['qid'], config['name'])
        if not csv_text: continue

        # 3. Match & Build Feature Collection
        features = []
        # Use set to avoid duplicate points if item has multiple classes
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
            except:
                continue

            osm_ref = osm_ids.get(qid)
            
            # --- STATUS LOGIC ---
            status = "missing"
            if osm_ref:
                status = "done"
            
            # Orange Logic: Only for specific "broad" classes (Mountains, etc.)
            # We ignore the P131 count to avoid false positives (e.g. 2 municipalities in same region)
            if item_class in BROAD_CLASSES:
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

        # Save to DATA folder
        outfile = os.path.join(DATA_DIR, f"data_{key}.geojson")
        with open(outfile, 'w', encoding='utf-8') as f:
            json.dump({"type": "FeatureCollection", "features": features}, f)
        
        print(f"   -> Saved: {outfile} ({len(features)} items)")
        processed_count += 1
        time.sleep(1)

    # Write Metadata
    if processed_count > 0:
        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M UTC")
        meta_content = { 
            "osm_date": latest_osm_date, 
            "wiki_date": now_str, 
            "regions_count": len(regions) 
        }
        with open("metadata.json", "w") as f:
            json.dump(meta_content, f)
        print(f"\nMetadata updated: OSM {latest_osm_date} / Wiki {now_str}")

    print(f"\nDONE.")

if __name__ == "__main__":
    main()
