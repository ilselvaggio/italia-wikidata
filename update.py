import json
import csv
import time
import requests
import os
import sys

# --- KONFIGURATION ---
REGIONS_FILE = "regions.json"
WIKIDATA_URL = "https://query.wikidata.org/sparql"

def get_wikidata_auto(qid, region_name):
    """Lädt Wikidata LIVE herunter"""
    query = f"""SELECT DISTINCT ?qid ?lat ?lon ?label WHERE {{ 
       ?item wdt:P131* wd:{qid}; wdt:P625 ?loc . 
       BIND(STRAFTER(STR(?item), '/entity/') as ?qid) 
       BIND(geof:latitude(?loc) as ?lat) 
       BIND(geof:longitude(?loc) as ?lon) 
       OPTIONAL {{ ?item rdfs:label ?label. FILTER(lang(?label)='it') }} 
    }}"""
    
    print(f"   -> Download Wikidata für {region_name} ({qid})...", end=" ", flush=True)
    
    try:
        # User-Agent ist wichtig, damit Wikidata uns nicht blockiert
        headers = {'User-Agent': 'ItaliaWikidataCheck/1.0', 'Accept': 'text/csv'}
        r = requests.get(WIKIDATA_URL, params={'query': query}, headers=headers)
        r.raise_for_status()
        print("OK.")
        return r.text
    except Exception as e:
        print(f"FEHLER: {e}")
        return None

def process_hybrid():
    # 1. Regions-Datei laden
    if not os.path.exists(REGIONS_FILE):
        print(f"FEHLER: {REGIONS_FILE} fehlt.")
        return

    with open(REGIONS_FILE, 'r', encoding='utf-8') as f:
        regions = json.load(f)

    print(f"Prüfe lokale OSM-Dateien für {len(regions)} Regionen...")
    processed_count = 0

    for key, config in regions.items():
        # Wir suchen NUR nach der lokalen OSM Datei. Wikidata holen wir uns frisch.
        file_osm = f"osm_{key}.json"

        if not os.path.exists(file_osm):
            # Wenn keine OSM Datei da ist, überspringen wir die Region stillschweigend
            # oder mit kurzer Info, damit der Screen nicht vollgespammt wird.
            continue

        print(f"\n--- Verarbeite {config['name']} ---")

        # A. OSM LOKAL LADEN
        osm_ids = {}
        try:
            with open(file_osm, 'r', encoding='utf-8') as f:
                data = json.load(f)
                elements = data.get('elements', [])
                
                if len(elements) == 0:
                    print("   WARNUNG: OSM Datei ist leer (0 Elemente). Überspringe.")
                    continue

                print(f"   Lese lokale OSM Datei: {len(elements)} Objekte.")
                
                for element in elements:
                    tags = element.get('tags', {})
                    el_type = element.get('type')
                    el_id = element.get('id')
                    osm_link_id = f"{el_type}/{el_id}"
                    
                    if 'wikidata' in tags:
                        # QID bereinigen
                        raw = tags['wikidata'].split(';')[0].strip().upper()
                        if raw.startswith('Q'):
                            osm_ids[raw] = osm_link_id
        except Exception as e:
            print(f"   FEHLER beim Lesen von {file_osm}: {e}")
            continue

        # B. WIKIDATA ONLINE LADEN
        csv_text = get_wikidata_auto(config['qid'], config['name'])
        if not csv_text:
            print("   Überspringe wegen Wikidata-Fehler.")
            continue

        # C. ABGLEICH
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
            except:
                continue

            osm_ref = osm_ids.get(qid)
            status = "done" if osm_ref else "missing"

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

        # D. SPEICHERN
        outfile = f"data_{key}.geojson"
        with open(outfile, 'w', encoding='utf-8') as f:
            json.dump({"type": "FeatureCollection", "features": features}, f)
        
        print(f"   -> ERGEBNIS: {outfile} erstellt ({len(features)} Items)")
        processed_count += 1
        
        # Kurze Pause um Wikidata nicht zu ärgern
        time.sleep(1)

    # Metadata Update
    if processed_count > 0:
        import datetime
        now = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")
        with open("metadata.json", "w") as f:
            json.dump({"last_updated": now, "regions_count": len(regions)}, f)

    print(f"\nFERTIG. {processed_count} Regionen erfolgreich verarbeitet.")

if __name__ == "__main__":
    process_hybrid()
