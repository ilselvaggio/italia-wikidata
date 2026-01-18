import json
import csv
import time
import requests
import os
import sys
import datetime
import argparse

# --- CONFIGURATION ---
REGIONS_FILE = "regions.json"
WIKIDATA_URL = "https://query.wikidata.org/sparql"
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OSM_DIR = "osm"             
DATA_DIR = "data_overpass"  
METADATA_FILE = "metadata.json"
BOUNDARIES_FILE = "regions_boundaries.geojson"

def get_bbox_from_feature(feature):
    """Berechnet min_lat, min_lon, max_lat, max_lon aus einem GeoJSON Feature"""
    all_coords = []
    
    def extract(coords_list):
        for item in coords_list:
            # Wenn es eine Koordinate ist [lon, lat]
            if isinstance(item, list) and len(item) == 2 and isinstance(item[0], (int, float)):
                all_coords.append(item)
            elif isinstance(item, list):
                extract(item)
    
    extract(feature['geometry']['coordinates'])
    
    if not all_coords: return None
    
    lons = [c[0] for c in all_coords]
    lats = [c[1] for c in all_coords]
    return (min(lats), min(lons), max(lats), max(lons))

def fetch_osm_bbox(bbox, retries=3):
    # bbox = (south, west, north, east)
    query = f"""
    [out:json][timeout:180];
    (
      node["wikidata"]({bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]});
      way["wikidata"]({bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]});
      relation["wikidata"]({bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]});
    );
    out tags qt;
    """
    for attempt in range(retries):
        try:
            response = requests.get(OVERPASS_URL, params={'data': query}, timeout=190)
            response.raise_for_status()
            data = response.json()
            if 'elements' in data: return data
        except Exception as e:
            print(f"      [!] Attempt {attempt+1}/{retries} failed: {e}")
            time.sleep(5)
    return None

def fetch_osm_area_fallback(area_id, retries=2):
    # Fallback zur alten Methode, falls GeoJSON fehlt
    query = f"""
    [out:json][timeout:180];
    area({area_id})->.searchArea;
    (
      node["wikidata"](area.searchArea);
      way["wikidata"](area.searchArea);
      relation["wikidata"](area.searchArea);
    );
    out tags qt;
    """
    for attempt in range(retries):
        try:
            response = requests.get(OVERPASS_URL, params={'data': query}, timeout=190)
            response.raise_for_status()
            return response.json()
        except: time.sleep(5)
    return None

def get_wikidata_clean(qid, region_name):
    query = f"""SELECT DISTINCT ?qid ?lat ?lon ?label WHERE {{
       ?item wdt:P131* wd:{qid}; wdt:P625 ?loc .
       FILTER NOT EXISTS {{ ?item wdt:P582 ?end. FILTER(?end < NOW()) }}
       FILTER NOT EXISTS {{ ?item wdt:P576 ?dissolved. FILTER(?dissolved < NOW()) }}
       MINUS {{ ?item p:P131 ?stmt . ?stmt pq:P582 ?linkEnd . FILTER(?linkEnd < NOW()) }}
       BIND(STRAFTER(STR(?item), '/entity/') as ?qid) 
       BIND(geof:latitude(?loc) as ?lat) 
       BIND(geof:longitude(?loc) as ?lon) 
       OPTIONAL {{ ?item rdfs:label ?label. FILTER(lang(?label)='en') }} 
       OPTIONAL {{ ?item rdfs:label ?label. FILTER(lang(?label)='it') }}
    }}"""
    
    print(f"   -> Downloading Wikidata ({qid})...", end=" ", flush=True)
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--region", default="all", help="Region key")
    args = parser.parse_args()

    if not os.path.exists(REGIONS_FILE): return
    if not os.path.exists(OSM_DIR): os.makedirs(OSM_DIR)
    if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)

    with open(REGIONS_FILE, 'r', encoding='utf-8') as f:
        regions = json.load(f)

    # Lade Grenzen für BBox Berechnung
    boundary_features = {}
    if os.path.exists(BOUNDARIES_FILE):
        try:
            with open(BOUNDARIES_FILE, 'r', encoding='utf-8') as f:
                gj = json.load(f)
                for feat in gj.get('features', []):
                    # Versuche OSM ID zu finden
                    props = feat.get('properties', {})
                    osm_id = props.get('id') or props.get('@id') or feat.get('id')
                    if osm_id:
                        osm_id = str(osm_id).replace('relation/', '')
                        boundary_features[osm_id] = feat
        except Exception as e:
            print(f"[Warn] Could not load boundaries: {e}")

    old_region_meta = {}
    if os.path.exists(METADATA_FILE):
        try:
            with open(METADATA_FILE, 'r') as f:
                old_region_meta = json.load(f).get("regions", {})
        except: pass

    target_regions = regions.keys() if args.region == 'all' else [args.region]
    print(f"--- Starting Update (Mode: BBox Fast) ---")
    
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M UTC")
    new_region_meta = old_region_meta.copy()
    processed_count = 0

    for key in target_regions:
        if key not in regions: continue
        config = regions[key]
        print(f"\n--- Processing {config['name']} ---")
        
        file_osm = os.path.join(OSM_DIR, f"osm_{key}.json")
        osm_data = None
        current_osm_date = "Unknown"
        
        # 1. Versuche BBox Strategy (Sehr schnell)
        # Wir brauchen die Relation ID aus der Config, um das Feature zu finden
        rel_id_str = str(int(config['osm']) - 3600000000)
        bbox = None
        
        if rel_id_str in boundary_features:
            bbox = get_bbox_from_feature(boundary_features[rel_id_str])
        
        if bbox:
            print(f"   -> Fetching OSM via BBox...", end=" ")
            sys.stdout.flush()
            new_data = fetch_osm_bbox(bbox)
        else:
            print(f"   -> Fallback: Fetching via Area (No BBox)...", end=" ")
            sys.stdout.flush()
            new_data = fetch_osm_area_fallback(config['osm'])

        if new_data:
            print("Success.")
            with open(file_osm, 'w', encoding='utf-8') as f:
                json.dump(new_data, f)
            osm_data = new_data
            current_osm_date = now_str
        else:
            print("Failed. Using cache.")
            if key in old_region_meta:
                old_date = old_region_meta[key].get("osm", "Unknown")
                if "(Cached)" not in old_date: current_osm_date = f"{old_date} (Cached)"
                else: current_osm_date = old_date
            
            if os.path.exists(file_osm):
                try:
                    with open(file_osm, 'r', encoding='utf-8') as f:
                        osm_data = json.load(f)
                except: pass

        if not osm_data:
            print("   [CRITICAL] No Data. Skipping.")
            continue

        new_region_meta[key] = { "osm": current_osm_date, "wiki": now_str }

        osm_ids = {}
        for el in osm_data.get('elements', []):
            if 'wikidata' in el.get('tags', {}):
                raw_tags = el['tags']['wikidata'].replace(',', ';')
                for raw in raw_tags.split(';'):
                    raw = raw.strip().upper()
                    if raw.startswith('Q'): osm_ids[raw] = f"{el['type']}/{el['id']}"

        # Wikidata
        csv_text = get_wikidata_clean(config['qid'], config['name'])
        if not csv_text: continue

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
            except: continue
            
            label = row.get('label') or row.get('?label') or qid
            status = "done" if osm_ids.get(qid) else "missing"
            
            features.append({
                "type": "Feature",
                "properties": { "wikidata": qid, "name": label, "status": status, "osm_id": osm_ids.get(qid) },
                "geometry": { "type": "Point", "coordinates": [lon, lat] }
            })
            seen.add(qid)

        with open(os.path.join(DATA_DIR, f"data_{key}.geojson"), 'w', encoding='utf-8') as f:
            json.dump({"type": "FeatureCollection", "features": features}, f)
        
        print(f"   -> Saved {len(features)} items")
        processed_count += 1
        time.sleep(1)

    if processed_count > 0:
        with open(METADATA_FILE, 'w') as f:
            json.dump({ "global_osm_date": now_str, "global_wiki_date": now_str, "regions": new_region_meta }, f)

    print("\nDONE.")

if __name__ == "__main__":
    main()
