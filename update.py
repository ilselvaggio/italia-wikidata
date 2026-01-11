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

def get_wikidata_smart(qid, region_name):
    # Diese Query macht zwei Dinge:
    # 1. Hauptsuche nach Objekten in der Region
    # 2. Zählen der P131-Werte (Verwaltungseinheiten), um "Orange" zu bestimmen
    
    query = f"""SELECT ?qid ?lat ?lon ?label ?p131count WHERE {{
       # 1. Finde Items in der Region
       ?item wdt:P131* wd:{qid}; wdt:P625 ?loc .
       
       # 2. Ausschlusskriterien (Historisches)
       # Objekt selbst darf nicht beendet sein
       FILTER NOT EXISTS {{ ?item wdt:P582 ?end. FILTER(?end < NOW()) }}
       FILTER NOT EXISTS {{ ?item wdt:P576 ?dissolved. FILTER(?dissolved < NOW()) }}
       
       # WICHTIG: Die Verbindung zum Ort (P131) darf nicht abgelaufen sein!
       # Das entfernt den Bauernhof, der nach Österreich "umgezogen" ist.
       FILTER NOT EXISTS {{ 
           ?item p:P131 ?stmt . 
           ?stmt pq:P582 ?linkEnd . 
           FILTER(?linkEnd < NOW()) 
       }}

       # 3. Zähle die GÜLTIGEN P131 Verbindungen (für Orange-Status)
       {{
           SELECT ?item (COUNT(?parent) AS ?p131count) WHERE {{
               ?item wdt:P131 ?parent .
           }} GROUP BY ?item
       }}

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

def get_file_date(filepath):
    """Holt das Änderungsdatum der OSM-Datei vom PC"""
    timestamp = os.path.getmtime(filepath)
    return datetime.datetime.fromtimestamp(timestamp).strftime("%d/%m/%Y %H:%M")

def main():
    if not os.path.exists(REGIONS_FILE):
        print(f"FEHLER: {REGIONS_FILE} fehlt.")
        return

    with open(REGIONS_FILE, 'r', encoding='utf-8') as f:
        regions = json.load(f)

    print(f"--- Starte Smart Update ---")
    
    processed_count = 0
    osm_date_str = "Unknown"

    for key, config in regions.items():
        file_osm = f"osm_{key}.json"
        
        if not os.path.exists(file_osm):
            continue

        # Datum der ersten gefundenen OSM Datei als Referenz nehmen
        if osm_date_str == "Unknown":
            osm_date_str = get_file_date(file_osm)

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
        csv_text = get_wikidata_smart(config['qid'], config['name'])
        if not csv_text: continue

        # 3. Abgleich
        features = []
        reader = csv.DictReader(csv_text.splitlines())
        
        for row in reader:
            qid = row.get('qid') or row.get('?qid')
            if not qid: continue
            qid = qid.split('/')[-1].upper()
            
            try:
                lat = float(row.get('lat') or row.get('?lat'))
                lon = float(row.get('lon') or row.get('?lon'))
                label = row.get('label') or row.get('?label') or qid
                # Anzahl der P131 Verbindungen
                p131_count = int(row.get('p131count') or row.get('?p131count') or 1)
            except:
                continue

            osm_ref = osm_ids.get(qid)
            status = "missing"
            if osm_ref: status = "done"
            
            # ORANGE LOGIK: Wenn mehr als 1 P131 (Verwaltungseinheit), dann Orange
            if p131_count > 1:
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

        outfile = f"data_{key}.geojson"
        with open(outfile, 'w', encoding='utf-8') as f:
            json.dump({"type": "FeatureCollection", "features": features}, f)
        
        print(f"   -> Gespeichert: {outfile} ({len(features)} Items)")
        processed_count += 1
        time.sleep(1)

    # Metadata schreiben
    if processed_count > 0:
        # Wikidata Zeit ist JETZT
        now = datetime.datetime.now()
        # +1 Stunde für UTC+1 Simulation (oder echte Zeitzone nutzen)
        wiki_date = now.strftime("%d/%m/%Y %H:%M")
        
        with open("metadata.json", "w") as f:
            json.dump({
                "osm_date": osm_date_str,
                "wiki_date": wiki_date,
                "regions_count": len(regions)
            }, f)

    print(f"\nFERTIG.")

if __name__ == "__main__":
    main()
