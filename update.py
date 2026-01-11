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

# Liste von Klassen (Q-Nummern), die ORANGE (Status: broad) werden sollen
# Q46831 = Gebirge, Q82794 = Geografische Region, Q123705 = Nachbarschaft/Viertel, Q205466 = Verkehrsweg
BROAD_CLASSES = ['Q46831', 'Q82794', 'Q123705', 'Q205466', 'Q15312'] 

def get_wikidata_auto(qid, region_name):
    """
    Lädt Wikidata LIVE herunter.
    Filtert historische Objekte (Enddatum < Heute) aus.
    """
    # SPARQL Query mit Filtern für P582 (Endzeit) und P576 (Auflösung)
    query = f"""SELECT DISTINCT ?qid ?lat ?lon ?label ?class WHERE {{ 
       ?item wdt:P131* wd:{qid}; wdt:P625 ?loc . 
       
       # Hole die Klasse (P31) für die Orange-Markierung
       OPTIONAL {{ ?item wdt:P31 ?classItem . BIND(STRAFTER(STR(?classItem), '/entity/') as ?class) }}

       # FILTER: Alles weg, was ein Enddatum in der Vergangenheit hat
       FILTER NOT EXISTS {{ ?item wdt:P582 ?end. FILTER(?end < NOW()) }}
       FILTER NOT EXISTS {{ ?item wdt:P576 ?dissolved. FILTER(?dissolved < NOW()) }}

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
        
        # Auf GitHub liegen die Dateien im Root, wir prüfen einfach ob sie da sind
        if not os.path.exists(file_osm):
            print(f"[Info] Keine OSM-Datei für {key}, überspringe.")
            continue

        print(f"\n--- Verarbeite {config['name']} ---")

        # 1. OSM Datei lesen
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

        # 2. Wikidata Live holen
        csv_text = get_wikidata_auto(config['qid'], config['name'])
        if not csv_text: continue

        # 3. Abgleich
        features = []
        processed_qids = set() # Um Duplikate zu vermeiden
        
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
            
            # Status Logik
            status = "missing"
            if osm_ref:
                status = "done"
            
            # Überschreibe Status mit "broad" (Orange), wenn es z.B. ein Gebirge ist
            # Aber nur, wenn es noch nicht "done" ist (oder willst du auch erledigte Gebirge orange haben?)
            # Hier: Gebirge werden IMMER orange, damit man sieht, dass es keine normalen Punkte sind.
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

        # 4. Speichern
        outfile = f"data_{key}.geojson"
        with open(outfile, 'w', encoding='utf-8') as f:
            json.dump({"type": "FeatureCollection", "features": features}, f)
        
        print(f"   -> Gespeichert: {outfile} ({len(features)} Items)")
        processed_count += 1
        time.sleep(1) # Kurze Pause für Wikidata API

    # Metadata schreiben
    if processed_count > 0:
        now_str = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")
        with open("metadata.json", "w") as f:
            json.dump({"last_updated": now_str, "regions_count": len(regions)}, f)

    print(f"\nFERTIG. {processed_count} Regionen aktualisiert.")

if __name__ == "__main__":
    main()
