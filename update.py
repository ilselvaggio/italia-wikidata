import json
import csv
import time
import datetime
import requests
import sys

# --- KONFIGURATION ---
REGIONS_FILE = "regions.json"
MAX_RETRIES = 5             # Hartnäckig bleiben
RETRY_DELAY = 10 
OVERPASS_TIMEOUT = 1200     # 20 Minuten Timeout

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
    
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, params={'query': query}, headers={'Accept': 'text/csv'})
            r.raise_for_status()
            return r.text
        except Exception as e:
            print(f"     Fehler Wikidata (Versuch {attempt+1}): {e}")
            time.sleep(5)
    return ""

def get_overpass(area_id):
    # --- PERFORMANCE QUERY ---
    # Wir nutzen api.openstreetmap.fr (OAPI).
    # Wir suchen NUR nach dem exakten Tag "wikidata".
    query = f"""
        [out:json][timeout:{OVERPASS_TIMEOUT}];
        area(id:{area_id})->.searchArea;
        (
          node["wikidata"](area.searchArea);
          way["wikidata"](area.searchArea);
          relation["wikidata"](area.searchArea);
        );
        out tags;
    """
    
    # Der französische Server ist oft der stabilste für Massendaten
    url = "https://api.openstreetmap.fr/oapi/interpreter"
    
    print(f"  -> Lade Overpass (France OAPI) per Area {area_id}...")
    
    for attempt in range(MAX_RETRIES):
        try:
            # Post request ist sicherer bei langen Queries
            r = requests.post(url, data={'data': query})
            r.raise_for_status()
            
            data = r.json()
            if 'elements' in data:
                return data
            else:
                # Manchmal sendet Overpass HTML Fehler als 200 OK
                raise ValueError("Antwort ist kein gültiges JSON mit Elements")
                
        except Exception as e:
            print(f"     Fehler Overpass (Versuch {attempt+1}): {e}")
            time.sleep(RETRY_DELAY * (attempt + 1)) # Wartezeit erhöhen
            
    return None

def process_region(key, config):
    print(f"\n--- Region: {config['name']} ---")
    
    csv_text = get_wikidata(config['qid'])
    if not csv_text: return None

    osm_json = get_overpass(config['osm'])
    if not osm_json: 
        print(f"!!! Keine OSM Daten für {config['name']} - ÜBERSPRINGE")
        return None

    # OSM IDs mappen
    osm_ids = {}
    elements = osm_json.get('elements', [])
    print(f"    OSM Objekte gefunden: {len(elements)}")
    
    # Sicherheitscheck: Wenn Trentino/Veneto/Lombardei < 100 Objekte hat, ist der Download kaputt
    if len(elements) < 100 and key in ['trentino_alto_adige', 'veneto', 'lombardia']:
        print("!!! WARNUNG: Verdächtig wenige Daten. Wahrscheinlich Server-Fehler. Überspringe Speichern.")
        return None
    
    for element in elements:
        tags = element.get('tags', {})
        el_type = element.get('type')
        el_id = element.get('id')
        osm_link_id = f"{el_type}/{el_id}"
        
        if 'wikidata' in tags:
            raw_val = tags['wikidata']
            qid = raw_val.split(';')[0].strip().upper()
            if qid.startswith('Q'):
                osm_ids[qid] = osm_link_id

    # Abgleich
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
        
    outfile = f"data_{key}.geojson"
    with open(outfile, 'w', encoding='utf-8') as f:
        json.dump({"type": "FeatureCollection", "features": features}, f)
        
    return True

def main():
    with open(REGIONS_FILE, 'r', encoding='utf-8') as f:
        regions = json.load(f)
        
    now = datetime.datetime.now(datetime.timezone.utc)
    cet_now = now + datetime.timedelta(hours=1)
    
    # Schleife durch alle Regionen
    for key, config in regions.items():
        process_region(key, config)
        # Kurze Pause für den Server
        time.sleep(2) 

    # Metadata schreiben
    with open("metadata.json", "w") as f:
        json.dump({
            "last_updated": cet_now.strftime("%d/%m/%Y %H:%M (UTC+1)"), 
            "regions_count": len(regions),
            "method": "Live Overpass (France)"
        }, f)

    print("\n--- FERTIG ---")

if __name__ == "__main__":
    main()
