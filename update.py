import json
import csv
import time
import datetime
import requests
import sys

# --- CONFIGURAZIONE ---
REGIONS_FILE = "regions.json"
MAX_RETRIES = 3
RETRY_DELAY = 10
# URL trovato nello screenshot (ex Kumi Systems)
OVERPASS_URL = "https://overpass.private.coffee/api/interpreter"

def get_wikidata(qid):
    # Query SPARQL standard
    query = f"""SELECT DISTINCT ?qid ?lat ?lon ?label WHERE {{ 
       ?item wdt:P131* wd:{qid}; wdt:P625 ?loc . 
       BIND(STRAFTER(STR(?item), '/entity/') as ?qid) 
       BIND(geof:latitude(?loc) as ?lat) 
       BIND(geof:longitude(?loc) as ?lon) 
       OPTIONAL {{ ?item rdfs:label ?label. FILTER(lang(?label)='it') }} 
    }}"""
    
    url = "https://query.wikidata.org/sparql"
    print(f"  -> Lade Wikidata per {qid}...")
    
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
    # QUERY VELOCE: Cerca solo il tag esatto "wikidata" (No Regex!)
    query = f"""
        [out:json][timeout:900];
        area(id:{area_id})->.searchArea;
        (
          node["wikidata"](area.searchArea);
          way["wikidata"](area.searchArea);
          relation["wikidata"](area.searchArea);
        );
        out tags;
    """
    
    print(f"  -> Lade Overpass (Private.coffee) per Area {area_id}...")
    
    headers = {
        'User-Agent': 'ItaliaWikidataCheck/1.0',
        'Referer': 'https://github.com/ilselvaggio/italia-wikidata'
    }

    for attempt in range(MAX_RETRIES):
        try:
            r = requests.post(OVERPASS_URL, data={'data': query}, headers=headers)
            r.raise_for_status()
            
            data = r.json()
            if 'elements' in data:
                return data
            else:
                raise ValueError("JSON valido ma senza 'elements'")
                
        except Exception as e:
            print(f"     Fehler Overpass (Versuch {attempt+1}): {e}")
            time.sleep(RETRY_DELAY)
            
    # Fallback se Private.coffee fallisce
    print("     ! Backup: provo server Francese...")
    try:
        r = requests.post("https://api.openstreetmap.fr/oapi/interpreter", data={'data': query})
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"     ! Backup fallito: {e}")
        return None

def process_region(key, config):
    print(f"\n--- Region: {config['name']} ---")
    
    csv_text = get_wikidata(config['qid'])
    if not csv_text: return None

    osm_json = get_overpass(config['osm'])
    if not osm_json: 
        print(f"!!! Nessun dato OSM per {config['name']}")
        return None

    # Mappatura IDs
    osm_ids = {}
    elements = osm_json.get('elements', [])
    print(f"    OSM oggetti trovati: {len(elements)}")
    
    for element in elements:
        tags = element.get('tags', {})
        el_type = element.get('type')
        el_id = element.get('id')
        osm_link_id = f"{el_type}/{el_id}"
        
        # Tag esatto
        if 'wikidata' in tags:
            # QID clean (Q123;Q456 -> primo)
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
        
    return features

def main():
    with open(REGIONS_FILE, 'r', encoding='utf-8') as f:
        regions = json.load(f)
        
    now = datetime.datetime.now(datetime.timezone.utc)
    cet_now = now + datetime.timedelta(hours=1)
    
    all_features = []
    
    for key, config in regions.items():
        res = process_region(key, config)
        if res:
            all_features.extend(res)
        time.sleep(1)

    if all_features:
        print(f"\n--- Salvataggio data_italia.geojson ({len(all_features)} oggetti) ---")
        with open("data_italia.geojson", "w", encoding='utf-8') as f:
            json.dump({"type": "FeatureCollection", "features": all_features}, f)

    with open("metadata.json", "w") as f:
        json.dump({
            "last_updated": cet_now.strftime("%d/%m/%Y %H:%M (UTC+1)"), 
            "regions_count": len(regions),
            "method": "Live Overpass (private.coffee)"
        }, f)

    print("\n--- FINITO ---")

if __name__ == "__main__":
    main()
