import json
import csv
import time
import requests
import os
import sys
import datetime

# --- KONFIGURATION ---
REGIONS_FILE = "regions.json"
WIKIDATA_URL = "https://query.wikidata.org/sparql"

# Klassen für ORANGE Markierung (Status: broad)
# Q46831=Gebirge, Q205466=Verkehrsweg.
# Entfernt: Q82794 (Region), da dies zu viele Treffer in Abruzzo gab.
BROAD_CLASSES = ['Q46831', 'Q205466', 'Q15312'] 

def get_wikidata_auto(qid, region_name):
    # NEU: MINUS-Filter entfernt Objekte, deren P131-Verbindung (Ort) ein Enddatum hat.
    # Das repariert den Bauernhof, der jetzt in Österreich steht.
    query = f"""SELECT DISTINCT ?qid ?lat ?lon ?label ?class WHERE {{ 
       ?item wdt:P131* wd:{qid}; wdt:P625 ?loc . 
       
       OPTIONAL {{ ?item wdt:P31 ?classItem . BIND(STRAFTER(STR(?classItem), '/entity/') as ?class) }}

       # 1. Das Objekt selbst darf nicht historisch sein
       FILTER NOT EXISTS {{ ?item wdt:P582 ?end. FILTER(?end < NOW()) }}
       FILTER NOT EXISTS {{ ?item wdt:P576 ?dissolved. FILTER(?dissolved < NOW()) }}

       # 2. Die VERBINDUNG zur Region (P131) darf nicht abgelaufen sein (Fix für Bauernhof)
       MINUS {{ ?item p:P131 ?stmt . ?stmt pq:P582 ?linkEnd . FILTER(?linkEnd < NOW()) }}

       BIND(STRAFTER(STR(?item), '/entity/') as ?qid) 
       BIND(geof:latitude(?loc) as ?lat) 
       BIND(geof:longitude(?loc) as ?lon) 
       
       OPTIONAL {{ ?item rdfs:label ?label. FILTER(lang(?label)='it') }} 
    }}"""
    
    print(f"   -> Download Wikidata für {region_name} ({qid})...", end=" ", flush=True)
    
    try:
        headers = {'User-Agent': 'ItaliaWikidataCheck/1.0', 'Accept': 'text/csv'}
        r = requests.get(WIKIDATA_URL, params={'query': query}, headers=headers)
        r.raise_for_status()
        print("OK.")
        return r.text
    except Exception as e:
        print(f"FEHLER: {e}")
        return None

def main():
    if not os.path.exists(REGIONS_FILE):
        print(f"FEHLER: {REGIONS_FILE} fehlt.")
        return

    with open(REGIONS_FILE, 'r', encoding='utf-8') as f:
        regions = json.load(f)

    print(f"--- Starte Update für {len(regions)} Regionen ---")
    
    processed_count = 0

    for key, config in regions.items():
        file_osm = f"osm_{key}.json"
        
        if not os.path.exists(file_osm):
            print(f"[Info] Keine OSM-Datei für {key}, überspringe.")
            continue

        print(f"\n--- Verarbeite {config['name']} ---")

        # 1. OSM
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
            print(f"   FEHLER OSM: {e}")
            continue

        # 2. Wikidata
        csv_text = get_wikidata_auto(config['qid'], config['name'])
        if not csv_text: continue

        # 3. Abgleich
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
            except:
                continue

            osm_ref = osm_ids.get(qid)
            status = "missing"
            if osm_ref: status = "done"
            
            # Orange Logic: Nur bestimmte Klassen (Gebirge etc.)
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
                "geometry": {
                    "type": "Point",
                    "coordinates": [lon, lat]
                }
            })
            processed_qids.add(qid)

        outfile = f"data_{key}.geojson"
        with open(outfile, 'w', encoding='utf-8') as f:
            json.dump({"type": "FeatureCollection", "features": features}, f)
        
        print(f"   -> Gespeichert: {outfile} ({len(features)} Items)")
        processed_count += 1
        time.sleep(1)

    # Metadata schreiben (Automatisch aktuelles Datum + UTC Hinweis)
    if processed_count > 0:
        now = datetime.datetime.now(datetime.timezone.utc)
        # Format: 11/01/2026 23:00 (UTC)
        time_str = now.strftime("%d/%m/%Y %H:%M (UTC)")
        
        with open("metadata.json", "w") as f:
            json.dump({
                "last_updated": time_str, 
                "regions_count": len(regions),
                "info_osm": "OSM: Manual Upload"
            }, f)

    print(f"\nFERTIG. {processed_count} Regionen aktualisiert.")

if __name__ == "__main__":
    main()
