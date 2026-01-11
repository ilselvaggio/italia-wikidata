import json
import csv
import time
import datetime
import requests
import sys
import os

# --- KONFIGURATION ---
REGIONS_FILE = "regions.json"
MAX_RETRIES = 3
RETRY_DELAY = 60 # Sekunden warten vor Neustart bei Fehler

def get_wikidata(qid):
    query = f"""SELECT DISTINCT ?qid ?lat ?lon ?label WHERE {{ 
       ?item wdt:P131* wd:{qid}; wdt:P625 ?loc . 
       BIND(STRAFTER(STR(?item), '/entity/') as ?qid) 
       BIND(geof:latitude(?loc) as ?lat) 
       BIND(geof:longitude(?loc) as ?lon) 
       OPTIONAL {{ ?item rdfs:label ?label. FILTER(lang(?label)='it') }} 
    }}"""
    
    url = "https://query.wikidata.org/sparql"
    print(f"  -> Lade Wikidata für {qid}...")
    r = requests.get(url, params={'query': query}, headers={'Accept': 'text/csv'})
    r.raise_for_status()
    return r.text

def get_overpass(area_id):
    # Query ohne Turbo-Shortcuts
    query = f'[out:json][timeout:900]; area(id:{area_id})->.searchArea; nwr[~".*wikidata$"~"."](area.searchArea); out tags;'
    url = "https://overpass-api.de/api/interpreter"
    
    print(f"  -> Lade Overpass für Area {area_id}...")
    # Versuche den Download mit Retry-Logik
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.post(url, data={'data': query})
            r.raise_for_status()
            
            # Prüfen ob valides JSON
            data = r.json()
            if 'elements' in data:
                return data
            else:
                raise ValueError("JSON ohne 'elements' Key")
                
        except Exception as e:
            print(f"     FEHLER (Versuch {attempt+1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
            else:
                raise e # Beim letzten Versuch aufgeben

def process_region(key, config):
    print(f"\n--- Starte {config['name']} ---")
    
    # 1. Daten holen
    try:
        csv_text = get_wikidata(config['qid'])
        osm_json = get_overpass(config['osm'])
    except Exception as e:
        print(f"!!! ABBRUCH für {config['name']}: {e}")
        return False

    # 2. OSM IDs sammeln
    osm_ids = {}
    for element in osm_json.get('elements', []):
        tags = element.get('tags', {})
        el_type = element.get('type')
        el_id = element.get('id')
        osm_link_id = f"{el_type}/{el_id}"
        
        for k, v in tags.items():
            if k.endswith("wikidata"):
                # Extrahiere alle Q-Nummern
                import re
                found = re.findall(r'Q\d+', v, re.IGNORECASE)
                for qid in found:
                    osm_ids[qid.upper()] = osm_link_id

    # 3. Wikidata abgleichen & GeoJSON bauen
    features = []
    missing_count = 0
    done_count = 0
    
    # CSV parsen
    lines = csv_text.splitlines()
    reader = csv.DictReader(lines)
    
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
            
        # Status prüfen
        osm_ref = osm_ids.get(qid)
        status = "done" if osm_ref else "missing"
        
        if status == "missing": missing_count += 1
        else: done_count += 1
        
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
        
    # 4. Speichern
    outfile = f"data_{key}.geojson"
    with open(outfile, 'w', encoding='utf-8') as f:
        json.dump({"type": "FeatureCollection", "features": features}, f)
        
    print(f"  -> Gespeichert: {outfile} (Missing: {missing_count}, Done: {done_count})")
    return True

def main():
    with open(REGIONS_FILE, 'r', encoding='utf-8') as f:
        regions = json.load(f)
        
    # Metadata update
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    
    success_count = 0
    for key, config in regions.items():
        if process_region(key, config):
            success_count += 1
        # Höflichkeitspause zwischen Regionen
        time.sleep(5)
            
    # Metadata schreiben
    with open("metadata.json", "w") as f:
        json.dump({"last_updated": now, "regions_count": len(regions)}, f)

    print(f"\n--- UPDATE KOMPLETT ({success_count}/{len(regions)} erfolgreich) ---")

if __name__ == "__main__":
    main()
