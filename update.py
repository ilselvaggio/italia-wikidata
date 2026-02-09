import json
import csv
import time
import requests
import os
import sys
import datetime
import argparse
import io  # WICHTIG: Für sicheres CSV-Parsing

REGIONS_FILE = "regions.json"
WIKIDATA_URL = "https://query.wikidata.org/sparql"
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OSM_DIR = "osm"
DATA_DIR = "data_overpass"
METADATA_FILE = "metadata.json"
BOUNDARIES_FILE = "regions_boundaries.geojson"
BLACKLIST_FILE = "blacklist.json"
HISTORY_FILE = "history.json"
CATEGORY_CONFIG_FILE = "category_config.json"

def build_qid_lookup(category_config):
    qid_lookup = {}
    for group_name, group_data in category_config.items():
        for subgroup_name, types_dict in group_data["subgroups"].items():
            for type_name, qids in types_dict.items():
                for qid in qids:
                    qid_lookup[qid] = {
                        "group": group_name,
                        "subgroup": subgroup_name,
                        "type": type_name
                    }
    return qid_lookup

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
    pad = 0.05 # Etwas Padding für Sicherheit
    return (min(lats)-pad, min(lons)-pad, max(lats)+pad, max(lons)+pad)

def fetch_osm_bbox(bbox, retries=3):
    # Fallback Methode: Rechtecksuche
    query = f"""
    [out:json][timeout:300];
    (
      node["wikidata"]({bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]});
      way["wikidata"]({bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]});
      relation["wikidata"]({bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]});
    );
    out tags qt;
    """
    for attempt in range(retries):
        try:
            response = requests.get(OVERPASS_URL, params={'data': query}, timeout=310)
            response.raise_for_status()
            data = response.json()
            if 'elements' in data: return data
        except Exception as e:
            print(f"      [!] BBox-Fetch gescheitert ({attempt+1}/{retries}): {e}")
            time.sleep(5)
    return None

def fetch_osm_area_smart(area_id, retries=2):
    # Hauptmethode: Suche innerhalb der genauen Grenze
    # Timeout erhöht auf 600s für große Regionen
    query = f"""
    [out:json][timeout:600];
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
            response = requests.get(OVERPASS_URL, params={'data': query}, timeout=610)
            response.raise_for_status()
            data = response.json()
            # Einfacher Check ob die Antwort valide aussieht
            if 'elements' in data: return data
        except Exception as e:
            print(f"      [!] Area-Fetch gescheitert ({attempt+1}/{retries}): {e}")
            time.sleep(5)
    return None

def get_wikidata_clean(qid, retries=3):
    query = f"""SELECT DISTINCT ?qid ?lat ?lon ?itemLabel ?type WHERE {{
       ?item wdt:P131* wd:{qid}; wdt:P625 ?loc .

       FILTER NOT EXISTS {{ ?item wdt:P582 ?end. FILTER(?end < NOW()) }}
       FILTER NOT EXISTS {{ ?item wdt:P576 ?dissolved. FILTER(?dissolved < NOW()) }}
       # Ausschluss von Pfarreien etc. falls nötig
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
    
    headers = {
        'User-Agent': 'ItaliaWikidataCheck/10.0 (https://ilselvaggio.github.io/italia-wikidata)', 
        'Accept-Encoding': 'gzip', 
        'Accept': 'text/csv'
    }
    
    for attempt in range(retries):
        try:
            r = requests.get(WIKIDATA_URL, params={'query': query}, headers=headers)
            r.raise_for_status()
            return r.text
        except requests.HTTPError as http_err:
            print(f"      [!] Wikidata HTTP Error: {http_err}")
            if r.status_code in {429, 503}:
                time.sleep(30) # Längere Wartezeit bei Überlastung
            else:
                time.sleep(5)
        except Exception as e:
            print(f"      [!] Wikidata Fetch Error: {e}")
            time.sleep(5)
    return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--region", default="all", help="Region key")
    args = parser.parse_args()

    # Ordner sicherstellen
    if not os.path.exists(REGIONS_FILE):
        print(f"CRITICAL: {REGIONS_FILE} nicht gefunden!")
        return
    for d in [OSM_DIR, DATA_DIR]:
        if not os.path.exists(d): os.makedirs(d)

    # Konfigurationen laden
    with open(REGIONS_FILE, 'r', encoding='utf-8') as f:
        regions = json.load(f)

    if os.path.exists(CATEGORY_CONFIG_FILE):
        with open(CATEGORY_CONFIG_FILE, "r", encoding="utf-8") as f:
            category_config = json.load(f)
        qid_lookup = build_qid_lookup(category_config)
    else:
        print("WARNUNG: category_config.json fehlt. Kategorien werden 'Altro' sein.")
        qid_lookup = {}

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
            with open(METADATA_FILE, 'r') as f: old_meta = json.load(f).get("regions", {})
        except: pass

    target_regions = regions.keys() if args.region == 'all' else [args.region]
    utc_now = datetime.datetime.now(datetime.timezone.utc)
    now_str = utc_now.isoformat()
    new_meta = old_meta.copy()
    processed_count = 0

    print(f"--- Starte Update für: {args.region} ---")

    for key in target_regions:
        if key not in regions: continue
        config = regions[key]
        print(f"\n[{processed_count+1}] Verarbeite {config['name']} ({key})...")

        file_osm = os.path.join(OSM_DIR, f"osm_{key}.json")
        osm_data = None
        current_osm_date = now_str
        
        # --- SCHRITT 1: OSM DATEN LADEN ---
        # Strategie: Zuerst Area versuchen (genauer). Wenn leer/Fehler -> BBox versuchen.
        
        # Versuch 1: Area
        print("   -> Lade OSM via Area ID...")
        osm_data = fetch_osm_area_smart(config['osm'])
        
        # Check ob valide Daten angekommen sind
        if not osm_data or 'elements' not in osm_data or len(osm_data['elements']) == 0:
            print("   -> Area fehlgeschlagen oder leer. Versuche Fallback (BBox)...")
            
            # Berechnung der BBox
            rel_id_str = str(int(config['osm']) - 3600000000)
            bbox = None
            if rel_id_str in boundary_features:
                bbox = get_bbox_from_feature(boundary_features[rel_id_str])
            
            if bbox:
                osm_data = fetch_osm_bbox(bbox)
            else:
                print("   -> Keine Boundary gefunden für BBox. Überspringe OSM-Update.")

        # Speichern oder altes laden
        if osm_data and 'elements' in osm_data:
            with open(file_osm, 'w', encoding='utf-8') as f: json.dump(osm_data, f)
        else:
            print("   -> OSM Update gescheitert. Nutze Cache falls vorhanden.")
            if key in old_meta: current_osm_date = old_meta[key].get("osm", "Unknown")
            if os.path.exists(file_osm):
                with open(file_osm, 'r', encoding='utf-8') as f: osm_data = json.load(f)

        if not osm_data: 
            print("   -> CRITICAL: Keine OSM Daten verfügbar. Überspringe Region.")
            continue

        # --- OSM IDs INDEXIEREN ---
        osm_ids = {}
        for el in osm_data.get('elements', []):
            tags = el.get('tags', {})
            if 'wikidata' in tags:
                # Splitten von Listen wie "Q1;Q2"
                wd_tags = tags['wikidata'].replace(',', ';').split(';')
                for raw in wd_tags:
                    raw = raw.strip().upper()
                    if raw.startswith('Q'): 
                        osm_ids[raw] = f"{el['type']}/{el['id']}"

        # --- SCHRITT 2: WIKIDATA LADEN ---
        print("   -> Lade Wikidata...")
        csv_text = get_wikidata_clean(config['qid'])
        
        if not csv_text: 
            print("   -> Wikidata Fehler. Überspringe.")
            continue

        features = []
        seen = set()
        
        # SICHERES CSV PARSING mit IO
        try:
            f_io = io.StringIO(csv_text)
            reader = csv.DictReader(f_io)
            
            for row in reader:
                qid = (row.get('qid') or row.get('?qid', '')).split('/')[-1].upper()
                if not qid or qid in seen or qid in blacklist: continue
                
                try:
                    lat = float(row.get('lat') or row.get('?lat'))
                    lon = float(row.get('lon') or row.get('?lon'))
                except: continue

                type_qid = (row.get('type') or row.get('?type', '')).split('/')[-1].upper()
                label_val = row.get('itemLabel') or row.get('?itemLabel') or qid

                # Kategorisierung
                if type_qid in qid_lookup:
                    match = qid_lookup[type_qid]
                    group = match["group"]
                    subgroup = match["subgroup"]
                    type_name = match["type"]
                else:
                    group = "Altro"
                    subgroup = "Altro"
                    type_name = "Altro"

                status = "done" if qid in osm_ids else "missing"

                features.append({
                    "type": "Feature",
                    "properties": {
                        "wikidata": qid,
                        "name": label_val,
                        "status": status,
                        "osm_id": osm_ids.get(qid),
                        "group": group,
                        "subgroup": subgroup,
                        "subcategory": type_name
                    },
                    "geometry": { "type": "Point", "coordinates": [lon, lat] }
                })
                seen.add(qid)
                
        except Exception as e:
            print(f"   -> FEHLER beim CSV Parsing: {e}")
            continue

        # --- SCHRITT 3: SPEICHERN ---
        done = sum(1 for f in features if f['properties']['status'] == 'done')
        total = len(features)
        
        # Metadaten update
        new_meta[key] = { 
            "osm": current_osm_date, 
            "wiki": now_str, 
            "done": done, 
            "total": total 
        }

        with open(os.path.join(DATA_DIR, f"data_{key}.geojson"), 'w', encoding='utf-8') as f:
            json.dump({"type": "FeatureCollection", "features": features}, f)

        print(f"   -> OK: {total} Objekte ({done} Matches)")
        processed_count += 1
        
        # Kurze Pause um API Rate Limits zu schonen
        time.sleep(2)

    # --- ABSCHLUSS ---
    if processed_count > 0:
        print("\nSpeichere Metadaten & Historie...")
        with open(METADATA_FILE, 'w') as f:
            json.dump({ "global_osm_date": now_str, "global_wiki_date": now_str, "regions": new_meta }, f)

        history = []
        if os.path.exists(HISTORY_FILE):
            try:
                with open(HISTORY_FILE, 'r') as f: history = json.load(f)
            except: pass

        today_str = datetime.datetime.now().strftime("%Y-%m-%d")
        # Alten Eintrag von heute entfernen (falls wir mehrmals am Tag laufen lassen)
        history = [h for h in history if h.get("date") != today_str]
        history.append({ "date": today_str, "data": new_meta })
        history.sort(key=lambda x: x['date'])

        with open(HISTORY_FILE, 'w') as f:
            json.dump(history, f)
            
    print("Update fertig.")

if __name__ == "__main__":
    main()
