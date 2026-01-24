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
BLACKLIST_FILE = "blacklist.json"

# --- CATEGORY MAPPING (30 Items + Località) ---
# Enthält erweiterte IDs (z.B. Nuraghe Varianten), um "Altro" zu minimieren.
CATEGORY_MAPPING = {
    "chiesa":               {"ids": ["Q16970"], "color": "#FF0000"},       # 1. Kirche
    "insediamento":         {"ids": ["Q486972"], "color": "#0000FF"},      # 2. Siedlung
    "frazione":             {"ids": ["Q1134686"], "color": "#008000"},     # 3. Frazione
    "cimitero":             {"ids": ["Q39614"], "color": "#808080"},       # 4. Friedhof
    "palazzo_comunale":     {"ids": ["Q25550691", "Q543654"], "color": "#FFA500"}, # 5. Rathaus (IT + General)
    "municipality_seat":    {"ids": ["Q193055"], "color": "#FFD700"},      # 6. Verwaltungssitz
    "biblio_pubblica":      {"ids": ["Q28564"], "color": "#00CED1"},       # 7. Öffentl. Bib.
    "cappella":             {"ids": ["Q108325"], "color": "#800080"},      # 8. Kapelle
    "nuraghe":              {"ids": ["Q688326", "Q688292", "Q2002347", "Q3924307"], "color": "#8B4513"}, # 9. Nuraghe (+Komplexe)
    "oratorio":             {"ids": ["Q1064047"], "color": "#FF69B4"},     # 10. Oratorium
    "villa":                {"ids": ["Q80966"], "color": "#4B0082"},       # 11. Villa
    "casa":                 {"ids": ["Q3947"], "color": "#A52A2A"},        # 12. Haus
    "natura_2000":          {"ids": ["Q2683204"], "color": "#32CD32"},     # 13. Natura 2000
    "struttura_arch":       {"ids": ["Q811979"], "color": "#708090"},      # 14. Arch. Struktur
    "palazzo":              {"ids": ["Q16560"], "color": "#DC143C"},       # 15. Palast/Palazzo
    "cascina":              {"ids": ["Q1046969"], "color": "#D2691E"},     # 16. Cascina
    "edificio":             {"ids": ["Q41176"], "color": "#C0C0C0"},       # 17. Gebäude
    "pianta_monum":         {"ids": ["Q811534"], "color": "#006400"},      # 18. Monumentalbaum
    "archivio":             {"ids": ["Q166118"], "color": "#4682B4"},      # 19. Archiv
    "palazzo_italiano":     {"ids": ["Q16560"], "color": "#B22222"},       # 20. (Mapping auf Palazzo Q16560 fallback)
    "castello":             {"ids": ["Q23413"], "color": "#800000"},       # 21. Schloss
    "montagna":             {"ids": ["Q8502"], "color": "#2F4F4F"},        # 22. Berg
    "biblio_spec":          {"ids": ["Q622549"], "color": "#20B2AA"},      # 23. Spez. Bib.
    "biblio_univ":          {"ids": ["Q856584"], "color": "#5F9EA0"},      # 24. Uni Bib.
    "capitello":            {"ids": ["Q750656"], "color": "#DA70D6"},      # 25. Bildstock
    "torre":                {"ids": ["Q12518"], "color": "#000080"},       # 26. Turm
    "museo":                {"ids": ["Q33506"], "color": "#DDA0DD"},       # 27. Museum
    "biblio_privata":       {"ids": ["Q7246255"], "color": "#BC8F8F"},     # 28. Privatbib.
    "strada_urbana":        {"ids": ["Q7944"], "color": "#696969"},        # 29. Stadtstraße
    "biblio_scolastica":    {"ids": ["Q193896"], "color": "#F0E68C"},      # 30. Schulbib.
    "localita":             {"ids": ["Q3257686"], "color": "#A0522D"},     # ZUSATZ: Località
    "other":                {"ids": [], "color": "#000000"}                # Fallback
}

def get_bbox_from_feature(feature):
    all_coords = []
    def extract(coords_list):
        for item in coords_list:
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

def get_wikidata_clean(qid):
    # Holt explizit das Label (?itemLabel) via Service
    query = f"""SELECT DISTINCT ?qid ?lat ?lon ?itemLabel ?type WHERE {{
       ?item wdt:P131* wd:{qid}; wdt:P625 ?loc .
       
       FILTER NOT EXISTS {{ ?item wdt:P582 ?end. FILTER(?end < NOW()) }}
       FILTER NOT EXISTS {{ ?item wdt:P576 ?dissolved. FILTER(?dissolved < NOW()) }}
       FILTER NOT EXISTS {{ ?item wdt:P5817 wd:Q56556915 }} 
       FILTER NOT EXISTS {{ ?item wdt:P5816 wd:Q56556915 }} 
       FILTER NOT EXISTS {{ ?item wdt:P5817 wd:Q11639308 }}

       MINUS {{ ?item p:P131 ?stmt . ?stmt pq:P582 ?linkEnd . FILTER(?linkEnd < NOW()) }}
       
       OPTIONAL {{ ?item wdt:P31 ?type. }}

       BIND(STRAFTER(STR(?item), '/entity/') as ?qid) 
       BIND(geof:latitude(?loc) as ?lat) 
       BIND(geof:longitude(?loc) as ?lon) 
       
       SERVICE wikibase:label {{ bd:serviceParam wikibase:language "it,en". }}
    }}"""
    try:
        headers = {'User-Agent': 'ItaliaWikidataCheck/5.0', 'Accept': 'text/csv'}
        r = requests.get(WIKIDATA_URL, params={'query': query}, headers=headers)
        r.raise_for_status()
        return r.text
    except: return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--region", default="all", help="Region key")
    args = parser.parse_args()

    if not os.path.exists(REGIONS_FILE): 
        print(f"Error: {REGIONS_FILE} not found.")
        return
    if not os.path.exists(OSM_DIR): os.makedirs(OSM_DIR)
    if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)

    with open(REGIONS_FILE, 'r', encoding='utf-8') as f:
        regions = json.load(f)

    blacklist = set()
    if os.path.exists(BLACKLIST_FILE):
        try:
            with open(BLACKLIST_FILE, 'r', encoding='utf-8') as f:
                blacklist = set(json.load(f))
        except: pass

    boundary_features = {}
    if os.path.exists(BOUNDARIES_FILE):
        try:
            with open(BOUNDARIES_FILE, 'r', encoding='utf-8') as f:
                gj = json.load(f)
                for feat in gj.get('features', []):
                    props = feat.get('properties', {})
                    osm_id = props.get('id') or props.get('@id') or feat.get('id')
                    if osm_id:
                        osm_id = str(osm_id).replace('relation/', '')
                        boundary_features[osm_id] = feat
        except: pass

    old_meta = {}
    if os.path.exists(METADATA_FILE):
        try:
            with open(METADATA_FILE, 'r') as f:
                old_meta = json.load(f).get("regions", {})
        except: pass

    target_regions = regions.keys() if args.region == 'all' else [args.region]
    utc_now = datetime.datetime.now(datetime.timezone.utc)
    now_str = utc_now.isoformat()
    
    new_meta = old_meta.copy()
    processed_count = 0

    for key in target_regions:
        if key not in regions: continue
        config = regions[key]
        print(f"\nProcessing {config['name']}...")
        
        file_osm = os.path.join(OSM_DIR, f"osm_{key}.json")
        osm_data = None
        current_osm_date = now_str
        
        rel_id_str = str(int(config['osm']) - 3600000000)
        bbox = get_bbox_from_feature(boundary_features[rel_id_str]) if rel_id_str in boundary_features else None
        
        if bbox:
            print("   -> Fetching OSM via BBox")
            new_data = fetch_osm_bbox(bbox)
        else:
            print("   -> Fetching OSM via Area (Fallback)")
            new_data = fetch_osm_area_fallback(config['osm'])

        if new_data:
            with open(file_osm, 'w', encoding='utf-8') as f: json.dump(new_data, f)
            osm_data = new_data
        else:
            if key in old_meta: current_osm_date = old_meta[key].get("osm", "Unknown")
            if os.path.exists(file_osm):
                with open(file_osm, 'r', encoding='utf-8') as f: osm_data = json.load(f)

        if not osm_data: continue

        osm_ids = {}
        for el in osm_data.get('elements', []):
            if 'wikidata' in el.get('tags', {}):
                for raw in el['tags']['wikidata'].replace(',', ';').split(';'):
                    raw = raw.strip().upper()
                    if raw.startswith('Q'): osm_ids[raw] = f"{el['type']}/{el['id']}"

        csv_text = get_wikidata_clean(config['qid'])
        if not csv_text: continue

        features = []
        seen = set()
        reader = csv.DictReader(csv_text.splitlines())
        for row in reader:
            qid = (row.get('qid') or row.get('?qid', '')).split('/')[-1].upper()
            if not qid or qid in seen or qid in blacklist: continue
            try:
                lat, lon = float(row.get('lat') or row.get('?lat')), float(row.get('lon') or row.get('?lon'))
            except: continue
            
            type_qid = (row.get('type') or row.get('?type', '')).split('/')[-1].upper()
            
            # WICHTIG: Nutzt das echte Label aus SPARQL
            label_val = row.get('itemLabel') or row.get('?itemLabel') or qid

            # Mapping Logic
            category_slug = "other"
            for cat_key, cat_val in CATEGORY_MAPPING.items():
                if type_qid in cat_val["ids"]:
                    category_slug = cat_key
                    break

            status = "done" if qid in osm_ids else "missing"
            features.append({
                "type": "Feature",
                "properties": { 
                    "wikidata": qid, 
                    "name": label_val, # Korrektes Label
                    "status": status, 
                    "osm_id": osm_ids.get(qid),
                    "category": category_slug # Nur Slug, kein 'type' mehr
                },
                "geometry": { "type": "Point", "coordinates": [lon, lat] }
            })
            seen.add(qid)

        done = sum(1 for f in features if f['properties']['status'] == 'done')
        total = len(features)
        new_meta[key] = { "osm": current_osm_date, "wiki": now_str, "done": done, "total": total }

        with open(os.path.join(DATA_DIR, f"data_{key}.geojson"), 'w', encoding='utf-8') as f:
            json.dump({"type": "FeatureCollection", "features": features}, f)
        
        print(f"   -> Saved {total} items ({done} matched)")
        processed_count += 1
        time.sleep(1)

    if processed_count > 0:
        with open(METADATA_FILE, 'w') as f:
            json.dump({ "global_osm_date": now_str, "global_wiki_date": now_str, "regions": new_meta }, f)

if __name__ == "__main__":
    main()
