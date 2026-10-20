import json
import csv
import time
import requests
import os
import sys
import datetime
import argparse

REGIONS_FILE = "regions.json"
WIKIDATA_URL = "https://query.wikidata.org/sparql"
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
    pad = 0.02
    return (min(lats)-pad, min(lons)-pad, max(lats)+pad, max(lons)+pad)

OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.private.coffee/api/interpreter",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]
OVERPASS_HEADERS = {
    # overpass-api.de lehnt anonyme/generische User-Agents (python-requests) ab
    'User-Agent': 'ItaliaWikidataBot/1.2 (https://github.com/ilselvaggio/italia-wikidata)',
    'Accept-Encoding': 'gzip',
}
OSM_FAILURES = []

def warn(msg):
    # Erscheint als Annotation im GitHub-Actions-Lauf (sichtbar ohne Logs zu öffnen)
    print(msg)
    if os.environ.get("GITHUB_ACTIONS"):
        print(f"::warning::{msg}")

def run_overpass(query, label):
    """Probiert alle Endpoints nacheinander. Gibt JSON nur zurück, wenn vollständig (kein Timeout-Remark)."""
    for url in OVERPASS_ENDPOINTS:
        for attempt in range(2):
            try:
                r = requests.post(url, data={'data': query}, headers=OVERPASS_HEADERS, timeout=420)
                if r.status_code in (429, 504):
                    print(f"      [!] {url} -> HTTP {r.status_code} ({label}), warte 60s")
                    time.sleep(60)
                    continue
                r.raise_for_status()
                data = r.json()
                remark = data.get('remark', '')
                if 'elements' not in data or 'error' in remark.lower():
                    print(f"      [!] {url} -> unvollständig ({label}): {remark[:150]}")
                    break  # gleicher Server wird gleich reagieren -> nächster Endpoint
                return data
            except Exception as e:
                print(f"      [!] {url} fehlgeschlagen ({label}, Versuch {attempt+1}/2): {str(e)[:200]}")
                time.sleep(15)
    return None

def osm_query(area_filter):
    return f"""
    [out:json][timeout:360];
    (
      node["wikidata"]{area_filter};
      way["wikidata"]{area_filter};
      relation["wikidata"]{area_filter};
    );
    out tags qt;
    """

def fetch_osm_bbox(bbox, depth=0):
    """Lädt eine BBox; bei Fehlschlag wird sie in 4 Kacheln geteilt (max. 2 Ebenen = 16 Kacheln)."""
    s, w, n, e = bbox
    data = run_overpass(osm_query(f"({s},{w},{n},{e})"), f"bbox depth {depth}")
    if data or depth >= 2:
        return data
    print(f"      -> Teile BBox in 4 Kacheln (Ebene {depth+1})")
    mlat, mlon = (s + n) / 2, (w + e) / 2
    merged, seen = None, set()
    for tile in [(s, w, mlat, mlon), (s, mlon, mlat, e), (mlat, w, n, mlon), (mlat, mlon, n, e)]:
        part = fetch_osm_bbox(tile, depth + 1)
        if not part:
            return None  # lieber Cache als unvollständige Daten
        if merged is None:
            merged = {k: v for k, v in part.items() if k != 'elements'}
            merged['elements'] = []
        for el in part['elements']:
            k = (el['type'], el['id'])
            if k not in seen:
                seen.add(k)
                merged['elements'].append(el)
        time.sleep(5)
    return merged

def fetch_osm_area_fallback(area_id):
    q = osm_query("(area.searchArea)").replace("(\n", f"area({area_id})->.searchArea;\n    (\n", 1)
    return run_overpass(q, "area")

def get_wikidata_clean(qid, retries=3):
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
    headers = {
        'User-Agent': 'ItaliaWikidataBot/1.1 (https://ilselvaggio.github.io/italia-wikidata)', 
        'Accept-Encoding': 'gzip', 
        'Accept': 'text/csv'
    }
    for attempt in range(retries):
        try:
            r = requests.get(WIKIDATA_URL, params={'query': query}, headers=headers, timeout=60)
            r.raise_for_status()
            return r.text
        except requests.HTTPError as http_err:
            print(f"      [!] Wikidata HTTP Fehler: {http_err} (Versuch {attempt+1}/{retries})")
            if r.status_code in {429, 503}:
                retry_after = r.headers.get('Retry-After')
                wait_time = int(retry_after) if retry_after and retry_after.isnumeric() else 15
                print(f"      [!] Rate Limit erreicht. Warte {wait_time} Sekunden.")
                time.sleep(wait_time)
            else:
                time.sleep(15)
        except Exception as e:
            print(f"      [!] Wikidata Fetch fehlgeschlagen (Versuch {attempt+1}/{retries}): {e}")
            time.sleep(15)
    return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--region", default="all", help="Region key")
    args = parser.parse_args()

    if not os.path.exists(REGIONS_FILE):
        print(f"Kritischer Fehler: {REGIONS_FILE} nicht gefunden.")
        sys.exit(1)
        
    os.makedirs(OSM_DIR, exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)

    with open(REGIONS_FILE, 'r', encoding='utf-8') as f:
        regions = json.load(f)

    with open(CATEGORY_CONFIG_FILE, "r", encoding="utf-8") as f:
        category_config = json.load(f)

    qid_lookup = build_qid_lookup(category_config)

    blacklist = set()
    if os.path.exists(BLACKLIST_FILE):
        try:
            with open(BLACKLIST_FILE, 'r', encoding='utf-8') as f:
                blacklist = set(json.load(f))
        except Exception as e:
            print(f"Warnung: Blacklist konnte nicht geladen werden ({e})")

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
        except Exception as e:
            print(f"Warnung: Boundaries konnten nicht geladen werden ({e})")

    old_meta = {}
    if os.path.exists(METADATA_FILE):
        try:
            with open(METADATA_FILE, 'r') as f:
                old_meta = json.load(f).get("regions", {})
        except:
            pass

    target_regions = regions.keys() if args.region == 'all' else [args.region]
    utc_now = datetime.datetime.now(datetime.timezone.utc)
    now_str = utc_now.isoformat()

    new_meta = old_meta.copy()
    processed_count = 0

    for key in target_regions:
        if key not in regions: 
            continue
            
        try:
            config = regions[key]
            print(f"\nVerarbeite {config['name']}...")

            file_osm = os.path.join(OSM_DIR, f"osm_{key}.json")
            osm_data = None
            current_osm_date = now_str

            rel_id_str = str(int(config['osm']) - 3600000000)
            bbox = get_bbox_from_feature(boundary_features[rel_id_str]) if rel_id_str in boundary_features else None

            if bbox:
                print("   -> Lade OSM Daten (via BBox)")
                new_data = fetch_osm_bbox(bbox)
            else:
                print("   -> Lade OSM Daten (via Area Fallback)")
                new_data = fetch_osm_area_fallback(config['osm'])

            if new_data:
                with open(file_osm, 'w', encoding='utf-8') as f: 
                    json.dump(new_data, f)
                osm_data = new_data
            else:
                warn(f"OSM-Download für {config['name']} fehlgeschlagen – nutze Cache vom {old_meta.get(key, {}).get('osm', '?')[:10]}")
                OSM_FAILURES.append(key)
                if key in old_meta: 
                    current_osm_date = old_meta[key].get("osm", "Unknown")
                if os.path.exists(file_osm):
                    with open(file_osm, 'r', encoding='utf-8') as f: 
                        osm_data = json.load(f)

            if not osm_data: 
                print("   -> Überspringe Region: Keine OSM-Daten verfügbar.")
                continue

            osm_ids = {}
            for el in osm_data.get('elements', []):
                if 'wikidata' in el.get('tags', {}):
                    for raw in el['tags']['wikidata'].replace(',', ';').split(';'):
                        raw = raw.strip().upper()
                        if raw.startswith('Q'): 
                            osm_ids[raw] = f"{el['type']}/{el['id']}"

            print("   -> Lade Wikidata...")
            csv_text = get_wikidata_clean(config['qid'])
            if not csv_text: 
                print("   -> Wikidata API fehlgeschlagen. Überspringe Region zum Schutz bestehender Daten.")
                continue

            features = []
            seen = set()
            reader = csv.DictReader(csv_text.splitlines())
            for row in reader:
                qid = (row.get('qid') or row.get('?qid', '')).split('/')[-1].upper()
                if not qid or qid in seen or qid in blacklist: 
                    continue
                try:
                    lat = float(row.get('lat') or row.get('?lat'))
                    lon = float(row.get('lon') or row.get('?lon'))
                except: 
                    continue

                type_qid = (row.get('type') or row.get('?type', '')).split('/')[-1].upper()
                label_val = row.get('itemLabel') or row.get('?itemLabel') or qid

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

            done = sum(1 for f in features if f['properties']['status'] == 'done')
            total = len(features)
            prev_total = old_meta.get(key, {}).get("total", 0)
            if prev_total and total < prev_total * 0.8:
                warn(f"Wikidata für {config['name']} lieferte nur {total} statt ~{prev_total} Items (abgeschnitten?) – Region übersprungen")
                continue
            new_meta[key] = { "osm": current_osm_date, "wiki": now_str, "done": done, "total": total }

            with open(os.path.join(DATA_DIR, f"data_{key}.geojson"), 'w', encoding='utf-8') as f:
                json.dump({"type": "FeatureCollection", "features": features}, f)

            print(f"   -> Gespeichert: {total} Items ({done} verknüpft)")
            processed_count += 1
            
            # Pause erhöht auf 5 Sekunden zur Schonung der Server
            time.sleep(5)
            
        except Exception as e:
            print(f"   [!] Unerwarteter Fehler bei der Verarbeitung von Region {key}: {e}")
            continue

    if processed_count > 0:
        # Metadaten JEDES MAL erzwingen zu speichern, indem wir einen "forced_update"-Zeitstempel einfügen
        with open(METADATA_FILE, 'w') as f:
            json.dump({ 
                "global_osm_date": now_str, 
                "global_wiki_date": now_str, 
                "last_run_forced": now_str, # Hiermit ändern sich die Metadaten garantiert immer
                "regions": new_meta 
            }, f)

        history = []
        if os.path.exists(HISTORY_FILE):
            try:
                with open(HISTORY_FILE, 'r') as f: 
                    history = json.load(f)
            except: 
                pass

        today_str = datetime.datetime.now().strftime("%Y-%m-%d")
        history = [h for h in history if h.get("date") != today_str]
        history.append({ "date": today_str, "data": new_meta })
        history.sort(key=lambda x: x['date'])

        with open(HISTORY_FILE, 'w') as f:
            json.dump(history, f)
            print(f"\n   -> History erfolgreich aktualisiert für {today_str}")
    else:
        print("\nKeine Regionen wurden erfolgreich verarbeitet. Die alten Daten bleiben bestehen.")

    if OSM_FAILURES:
        warn(f"OSM fehlgeschlagen für {len(OSM_FAILURES)} Region(en): {', '.join(OSM_FAILURES)}")

if __name__ == "__main__":
    main()
