import json
import csv
import time
import datetime
import requests
import os
import sys
import osmium
import ujson # Schnellerer JSON parser

# --- KONFIGURATION ---
REGIONS_FILE = "regions.json"
PBF_URL = "https://download.geofabrik.de/europe/italy-latest.osm.pbf"
PBF_FILE = "italy.osm.pbf"

class WikidataHandler(osmium.SimpleHandler):
    def __init__(self):
        super(WikidataHandler, self).__init__()
        # Speichert: wikidata_qid -> osm_id_string
        self.wikidata_map = {}

    def process_tags(self, obj, osm_type):
        # Wir suchen primär nach dem tag 'wikidata'
        # Falls du UNBEDINGT auch brand:wikidata willst, kostet das Performance,
        # aber lokal ist das machbar. Hier die schnelle Variante:
        if 'wikidata' in obj.tags:
            qid = obj.tags['wikidata']
            # Q-Nummer normalisieren (Großbuchstaben, Check Format)
            if qid.startswith('Q') or qid.startswith('q'):
                clean_qid = qid.upper().split(';')[0] # Nimm erstes falls mehrere
                self.wikidata_map[clean_qid] = f"{osm_type}/{obj.id}"

    def node(self, n):
        self.process_tags(n, "node")

    def way(self, w):
        self.process_tags(w, "way")

    def relation(self, r):
        self.process_tags(r, "relation")

def download_file(url, filename):
    print(f"-> Lade PBF Dump: {url}")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    print(f"-> Download fertig: {os.path.getsize(filename) / (1024*1024):.1f} MB")

def get_wikidata_sparql(qid):
    # Holt ALLE Objekte der Region auf einmal
    query = f"""SELECT DISTINCT ?qid ?lat ?lon ?label WHERE {{ 
       ?item wdt:P131* wd:{qid}; wdt:P625 ?loc . 
       BIND(STRAFTER(STR(?item), '/entity/') as ?qid) 
       BIND(geof:latitude(?loc) as ?lat) 
       BIND(geof:longitude(?loc) as ?lon) 
       OPTIONAL {{ ?item rdfs:label ?label. FILTER(lang(?label)='it') }} 
    }}"""
    
    url = "https://query.wikidata.org/sparql"
    for _ in range(3): # Retry logic
        try:
            r = requests.get(url, params={'query': query}, headers={'Accept': 'text/csv'})
            r.raise_for_status()
            return r.text
        except Exception as e:
            print(f"   Fehler Wikidata {qid}: {e}. Warte kurz...")
            time.sleep(5)
    return ""

def main():
    # 1. Regions-Config laden
    with open(REGIONS_FILE, 'r', encoding='utf-8') as f:
        regions = json.load(f)

    # 2. OSM Dump herunterladen (Nur einmal!)
    if not os.path.exists(PBF_FILE):
        download_file(PBF_URL, PBF_FILE)

    # 3. OSM Dump lokal verarbeiten (Scannen)
    print("-> Starte OSM-Scan (Pyosmium)... das dauert ca. 2-3 Minuten...")
    handler = WikidataHandler()
    handler.apply_file(PBF_FILE, locations=False) # locations=False spart RAM, wir brauchen keine Geometrie aus OSM, nur die ID
    
    print(f"-> Scan beendet. {len(handler.wikidata_map)} verknüpfte Objekte in Italien gefunden.")
    
    # 4. Abgleich pro Region
    all_italy_features = []
    success_count = 0
    
    for key, config in regions.items():
        print(f"\n--- Verarbeite {config['name']} ---")
        csv_text = get_wikidata_sparql(config['qid'])
        
        if not csv_text:
            print("   Keine Wikidata-Daten. Überspringe.")
            continue

        features = []
        missing_count = 0
        done_count = 0
        
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

            # STATUS CHECK (Lokaler Lookup im Dictionary)
            osm_ref = handler.wikidata_map.get(qid)
            status = "done" if osm_ref else "missing"
            
            if status == "missing": missing_count += 1
            else: done_count += 1
            
            feat = {
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
            }
            features.append(feat)
            all_italy_features.append(feat)
        
        # Speichern
        outfile = f"data_{key}.geojson"
        with open(outfile, 'w', encoding='utf-8') as f:
            ujson.dump({"type": "FeatureCollection", "features": features}, f)
        print(f"   Gespeichert: {len(features)} Objekte (Done: {done_count})")
        success_count += 1
        time.sleep(1) # Schonung Wikidata API

    # 5. Italien-Datei
    print("\n-> Speichere data_italia.geojson...")
    with open("data_italia.geojson", "w", encoding='utf-8') as f:
        ujson.dump({"type": "FeatureCollection", "features": all_italy_features}, f)

    # 6. Metadata
    now = datetime.datetime.now(datetime.timezone.utc)
    cet_now = now + datetime.timedelta(hours=1)
    
    with open("metadata.json", "w") as f:
        json.dump({
            "last_updated": cet_now.strftime("%d/%m/%Y %H:%M (UTC+1)"), 
            "regions_count": len(regions),
            "method": "Offline PBF Dump"
        }, f)

    print("\n--- FERTIG ---")
    # PBF löschen um Platz zu sparen (im Runner egal, aber sauber)
    os.remove(PBF_FILE)

if __name__ == "__main__":
    main()
