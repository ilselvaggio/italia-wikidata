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

def get_wikidata_clean(qid, region_name):
    # Query: Holt alle Items, filtert Historisches + Bauernhof-Fix
    query = f"""SELECT DISTINCT ?qid ?lat ?lon ?label WHERE {{
       # 1. Basis-Suche in der Region
       ?item wdt:P131* wd:{qid}; wdt:P625 ?loc .

       # 2. Historische Objekte ausschließen (Endzeitpunkt oder Aufgelöst)
       FILTER NOT EXISTS {{ ?item wdt:P582 ?end. FILTER(?end < NOW()) }}
       FILTER NOT EXISTS {{ ?item wdt:P576 ?dissolved. FILTER(?dissolved < NOW()) }}
       
       # 3. DEIN BAUERNHOF-FIX (Exakt wie gewünscht)
       # Entfernt Items, deren Verknüpfung zu DIESER Region abgelaufen ist
       MINUS {{ 
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
    
    print(f"   -> Downloading for {region_name} ({qid})...", end=" ", flush=True)
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
    if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)

    with open(REGIONS_FILE, 'r', encoding='utf-8') as f:
        regions = json.load(f)

    print(f"--- Starting Update (No Orange, Farm Fix Active) ---")
    processed = 0
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M UTC")

    for key, config in regions.items():
        file_osm = os.path.join(OSM_DIR, f"osm_{key}.json")
        if not os.path.exists(file_osm): continue
        
        print(f"\n--- Processing {config['name']} ---")

        # 1. OSM Laden
        osm_ids = {}
        try:
            with open(file_osm, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for el in data.get('elements', []):
                    if 'wikidata' in el.get('tags', {}):
                        raw = el['tags']['wikidata'].split(';')[0].strip().upper()
                        if raw.startswith('Q'): osm_ids[raw] = f"{el['type']}/{el['id']}"
        except: continue

        # 2. Wikidata Laden
        csv_text = get_wikidata_clean(config['qid'], config['name'])
        if not csv_text: continue

        # 3. Abgleich (Nur noch missing oder done)
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
            
            # Status Logik: Entweder rot (missing) oder grün (done)
            # Orange gibt es nicht mehr.
            status = "missing"
            if osm_ref: status = "done"

            features.append({
                "type": "Feature",
                "properties": { "wikidata": qid, "name": label, "status": status, "osm_id": osm_ref },
                "geometry": { "type": "Point", "coordinates": [lon, lat] }
            })
            seen.add(qid)

        outfile = os.path.join(DATA_DIR, f"data_{key}.geojson")
        with open(outfile, 'w', encoding='utf-8') as f:
            json.dump({"type": "FeatureCollection", "features": features}, f)
        
        print(f"   -> Saved {len(features)} items.")
        processed += 1
        time.sleep(1)

    if processed > 0:
        with open("metadata.json", "w") as f:
            json.dump({ "osm_date": now_str, "wiki_date": now_str, "regions_count": len(regions) }, f)

    print(f"\nDONE.")

if __name__ == "__main__":
    main()
