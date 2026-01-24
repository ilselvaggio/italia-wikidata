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

# --- KATEGORIE-HIERARCHIE ---
# Format: "Gruppe": { "color": "HEX", "subcats": { "Label": ["QID", ...] } }
CATEGORY_CONFIG = {
    "Religione": {
        "color": "#E63946", # Rot
        "subcats": {
            "Chiesa": ["Q16970", "Q317557", "Q55876909", "Q1088552", "Q19899465", "Q57644089", "Q96352496", "Q24398318"],
            "Cappella": ["Q108325", "Q1457501"],
            "Oratorio": ["Q1064047", "Q580499"],
            "Monastero/Convento": ["Q44613", "Q1128397", "Q160742", "Q513550"],
            "Santuario": ["Q697295"]
        }
    },
    "Archeologia": {
        "color": "#8D6E63", # Braun
        "subcats": {
            "Nuraghe": ["Q688326", "Q688292", "Q2002347", "Q3924307", "Q1385277"],
            "Tomba dei Giganti": ["Q1523627"],
            "Domus de Janas": ["Q782970"],
            "Sito Archeologico": ["Q839954", "Q109607", "Q15661340", "Q3363945", "Q200141", "Q14752696"]
        }
    },
    "Cultura": {
        "color": "#FFB703", # Gelb/Gold
        "subcats": {
            "Museo": ["Q33506", "Q124830213", "Q12104174", "Q614316", "Q207694", "Q16735822", "Q135713224", "Q3329412", "Q92755865", "Q108860593", "Q1970365", "Q2087181", "Q2398990", "Q1662089", "Q3867560", "Q124830411", "Q112132534", "Q112132542", "Q1231888", "Q112132527", "Q740437"],
            "Biblioteca": ["Q28564", "Q124750618", "Q385994", "Q380829", "Q1622062", "Q1076099", "Q7075", "Q105763925", "Q124750593", "Q124750614", "Q124750711", "Q2326815"],
            "Archivio": ["Q166118", "Q604177", "Q3621673", "Q17620767", "Q2877653"],
            "Teatro/Cinema": ["Q24354", "Q41253"]
        }
    },
    "Istruzione": {
        "color": "#FFD166", # Hellgelb
        "subcats": {
            "Scuola": ["Q126807", "Q9842", "Q149566", "Q57775518", "Q9826", "Q1244442", "Q3831968", "Q3803834", "Q3803808", "Q56177191", "Q233324"]
        }
    },
    "Amministrazione": {
        "color": "#F4A261", # Orange
        "subcats": {
            "Municipio": ["Q25550691", "Q15303838", "Q543654", "Q1137809"]
        }
    },
    "Fortificazioni": {
        "color": "#606C38", # Olivgrün
        "subcats": {
            "Castello": ["Q23413", "Q1408475", "Q1195705"],
            "Torre": ["Q12518", "Q200334"],
            "Mura/Porta/Forte": ["Q16748868", "Q82117", "Q57821", "Q1785071", "Q57346", "Q81851", "Q131263"]
        }
    },
    "Dimore ed Edifici": {
        "color": "#A53860", # Weinrot
        "subcats": {
            "Palazzo": ["Q16560", "Q2651004"],
            "Villa": ["Q80966", "Q3950", "Q111189432", "Q3558938"],
            "Casa": ["Q3947", "Q16884952"],
            "Edificio/Cascina": ["Q41176", "Q1169748", "Q1497375", "Q1662011", "Q35112127", "Q3694735", "Q3044808", "Q1303167", "Q3973051", "Q811979"]
        }
    },
    "Natura": {
        "color": "#2A9D8F", # Türkis
        "subcats": {
            "Montagna/Valle": ["Q8502", "Q39816", "Q46831", "Q207326", "Q133056", "Q54050"],
            "Acqua (Lago/Fiume/Spiaggia)": ["Q4735538", "Q23397", "Q4022", "Q34038", "Q40080", "Q12284", "Q185113"],
            "Parco/Area Protetta": ["Q15069452", "Q473972", "Q179049", "Q3936950", "Q3936952", "Q22698", "Q22746", "Q1107656", "Q167346", "Q4421"],
            "Albero Monumentale": ["Q811534"],
            "Grotta": ["Q35509"]
        }
    },
    "Insediamenti": {
        "color": "#457B9D", # Blau
        "subcats": {
            "Insediamento/Frazione": ["Q486972", "Q1134686", "Q3835961", "Q3257686", "Q123705", "Q676050"]
        }
    },
    "Infrastrutture": {
        "color": "#7D8597", # Grau-Blau
        "subcats": {
            "Cimitero": ["Q39614", "Q381885", "Q200141", "Q56055312"],
            "Trasporti": ["Q55488", "Q55678", "Q2175765", "Q12280", "Q537127", "Q181348", "Q2354973", "Q79007", "Q34442", "Q44782", "Q39715", "Q181348"],
            "Ospedale/Salute": ["Q16917", "Q4287745"],
            "Albergo/Rifugio": ["Q27686", "Q182676"],
            "Industria/Servizi": ["Q1662011", "Q329683", "Q44494", "Q13226383", "Q1076486"]
        }
    },
    "Monumenti e Spazi": {
        "color": "#9D4EDD", # Lila
        "subcats": {
            "Monumento": ["Q4989906", "Q5003624", "Q1885014", "Q56055312", "Q114124381", "Q721747", "Q11734477", "Q26703203", "Q575759", "Q3238324", "Q75762"],
            "Fontana/Lavatoio": ["Q483453", "Q1690211"],
            "Piazza/Elementi": ["Q174782", "Q185600", "Q391414", "Q179700", "Q860861", "Q241045", "Q16887380", "Q3305213", "Q281460", "Q3395121", "Q750656"]
        }
    }
}

# Lookup-Map für schnelle Zuweisung generieren
QID_LOOKUP = {}
for group, g_data in CATEGORY_CONFIG.items():
    for subcat, qids in g_data["subcats"].items():
        for qid in qids:
            QID_LOOKUP[qid] = {"group": group, "subcat": subcat}

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
    # FIX: Puffer hinzufügen, um Grenzüberschreitende Relationen besser zu finden
    pad = 0.02
    return (min(lats)-pad, min(lons)-pad, max(lats)+pad, max(lons)+pad)

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
    # Holt ?itemLabel für korrekte Namen
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
        headers = {'User-Agent': 'ItaliaWikidataCheck/6.0', 'Accept': 'text/csv'}
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
            label_val = row.get('itemLabel') or row.get('?itemLabel') or qid

            # --- ZUWEISUNG LOGIK ---
            if type_qid in QID_LOOKUP:
                match = QID_LOOKUP[type_qid]
                group = match["group"]
                subcat = match["subcat"]
            else:
                group = "Altro"
                subcat = "Altro"

            status = "done" if qid in osm_ids else "missing"
            
            features.append({
                "type": "Feature",
                "properties": { 
                    "wikidata": qid, 
                    "name": label_val, 
                    "status": status, 
                    "osm_id": osm_ids.get(qid),
                    "group": group,       # Hauptgruppe (Accordion)
                    "subcategory": subcat # Unterkategorie (Filter)
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
