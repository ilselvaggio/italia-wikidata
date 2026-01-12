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

# NUR DIESE TYPEN WERDEN ORANGE (Broad/Geografisch)
# Q46831 (Gebirge), Q205466 (Verkehrsweg), Q15312 (Pass), Q165 (Meer), Q4022 (Fluss)
BROAD_CLASSES = ['Q46831', 'Q205466', 'Q15312', 'Q165', 'Q4022', 'Q355304'] 

def get_wikidata_optimized(qid, region_name):
    # Optimierte Query mit MINUS statt komplexen Filtern
    query = f"""SELECT DISTINCT ?qid ?lat ?lon ?label ?class WHERE {{
       ?item wdt:P131* wd:{qid}; wdt:P625 ?loc .
       
       # Klasse holen (für Orange-Logik)
       OPTIONAL {{ ?item wdt:P31 ?classItem . BIND(STRAFTER(STR(?classItem), '/entity/') as ?class) }}

       # FILTER: Keine historischen Objekte
       FILTER NOT EXISTS {{ ?item wdt:P582 ?end. FILTER(?end < NOW()) }}
       FILTER NOT EXISTS {{ ?item wdt:P576 ?dissolved. FILTER(?dissolved < NOW()) }}
       
       # FILTER: Der Bauernhof-Fix (Verbindung zur Region abgelaufen)
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
    
    print(f"   -> Downloading optimized for {region_name} ({qid})...", end=" ", flush=True)
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

    print(f"--- Starting Update ---")
    processed = 0
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M UTC")

    for key, config in regions.items():
        file_osm = os.path.join(OSM_DIR, f"osm_{key}.json")
        if not os.path.exists(file_osm): continue
        
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
        csv_text = get_wikidata_optimized(config['qid'], config['name'])
        if not csv_text: continue

        # 3. Verarbeiten
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
                cls = row.get('class') or row.get('?class')
            except: continue

            osm_ref = osm_ids.get(qid)
            status = "missing"
            if osm_ref: status = "done"
            
            # ORANGE LOGIK: Nur noch strikt nach Klasse.
            # "Doppelte P131" werden ignoriert (bleiben rot/grün), um Fehler zu vermeiden.
            if cls in BROAD_CLASSES:
                status = "broad"

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

if __name__ == "__main__":
    main()
