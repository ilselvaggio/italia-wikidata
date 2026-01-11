import json
import csv
import time
import datetime
import requests
import sys
import os

# --- CONFIGURAZIONE ---
REGIONS_FILE = "regions.json"
MAX_RETRIES = 3
RETRY_DELAY = 10
OVERPASS_TIMEOUT = 900

def get_wikidata(qid):
    # Query SPARQL stretta sulla regione (P131)
    query = f"""SELECT DISTINCT ?qid ?lat ?lon ?label WHERE {{ 
       ?item wdt:P131* wd:{qid}; wdt:P625 ?loc . 
       BIND(STRAFTER(STR(?item), '/entity/') as ?qid) 
       BIND(geof:latitude(?loc) as ?lat) 
       BIND(geof:longitude(?loc) as ?lon) 
       OPTIONAL {{ ?item rdfs:label ?label. FILTER(lang(?label)='it') }} 
    }}"""
    
    url = "https://query.wikidata.org/sparql"
    print(f"  -> Scarico Wikidata per {qid}...")
    
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, params={'query': query}, headers={'Accept': 'text/csv'})
            r.raise_for_status()
            return r.text
        except Exception as e:
            print(f"     Errore Wikidata (Tentativo {attempt+1}): {e}")
            time.sleep(5)
    raise Exception("Impossibile scaricare da Wikidata dopo vari tentativi")

def get_overpass(area_id):
    # Query Overpass ottimizzata con timeout alto e maxsize
    query = f'[out:json][timeout:{OVERPASS_TIMEOUT}][maxsize:2073741824]; area(id:{area_id})->.searchArea; nwr["wikidata"](area.searchArea); out tags;'
    url = "https://overpass.private.coffee/api/interpreter"
    
    print(f"  -> Scarico Overpass per Area {area_id}...")
    
    for attempt in range(MAX_RETRIES):
        try:
            # Usiamo POST per evitare problemi di lunghezza URL
            r = requests.post(url, data={'data': query})
            r.raise_for_status()
            
            data = r.json()
            # Controllo integrità
            if 'elements' in data:
                return data
            else:
                if 'remark' in data:
                    print(f"     Overpass Remark: {data['remark']}")
                raise ValueError("JSON valido ma senza chiave 'elements'")
                
        except Exception as e:
            print(f"     Errore Overpass (Tentativo {attempt+1}): {e}")
            time.sleep(RETRY_DELAY * (attempt + 1)) # Backoff incrementale
            
    raise Exception("Impossibile scaricare da Overpass")

def process_region(key, config):
    print(f"\n--- Elaborazione: {config['name']} ---")
    
    try:
        csv_text = get_wikidata(config['qid'])
        osm_json = get_overpass(config['osm'])
    except Exception as e:
        print(f"!!! SALTO REGIONE {config['name']}: {e}")
        return None

    # Mappa OSM IDs
    osm_ids = {}
    elements = osm_json.get('elements', [])
    print(f"    Elementi OSM trovati: {len(elements)}")
    
    for element in elements:
        tags = element.get('tags', {})
        el_type = element.get('type')
        el_id = element.get('id')
        osm_link_id = f"{el_type}/{el_id}"
        
        for k, v in tags.items():
            if k.endswith("wikidata"):
                import re
                found = re.findall(r'Q\d+', v, re.IGNORECASE)
                for qid in found:
                    osm_ids[qid.upper()] = osm_link_id

    # Processa Wikidata
    features = []
    missing_count = 0
    done_count = 0
    
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
        
    # Salva file regionale
    outfile = f"data_{key}.geojson"
    with open(outfile, 'w', encoding='utf-8') as f:
        json.dump({"type": "FeatureCollection", "features": features}, f)
        
    print(f"  -> Salvato {outfile}: {missing_count} mancanti, {done_count} completati.")
    return features

def main():
    with open(REGIONS_FILE, 'r', encoding='utf-8') as f:
        regions = json.load(f)
        
    # Data e Ora UTC+1 (Corretto per evitare warning)
    utc_now = datetime.datetime.now(datetime.timezone.utc)
    cet_now = utc_now + datetime.timedelta(hours=1)
    
    all_italy_features = []
    success_count = 0
    
    for key, config in regions.items():
        region_features = process_region(key, config)
        if region_features is not None:
            success_count += 1
            all_italy_features.extend(region_features)
        
        # Pausa di cortesia per Overpass
        time.sleep(5)

    # Genera file "Tutta Italia"
    if all_italy_features:
        print(f"\n--- Generazione file Italia completa ({len(all_italy_features)} oggetti) ---")
        with open("data_italia.geojson", "w", encoding='utf-8') as f:
            json.dump({"type": "FeatureCollection", "features": all_italy_features}, f)
            
    # Metadata
    with open("metadata.json", "w") as f:
        json.dump({"last_updated": timestamp_str, "regions_count": len(regions)}, f)

    print(f"\n--- AGGIORNAMENTO COMPLETATO ({success_count}/{len(regions)} regioni) ---")

if __name__ == "__main__":
    main()
