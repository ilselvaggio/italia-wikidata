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
HISTORY_FILE = "history.json"

# --- 3-LEVEL CATEGORY CONFIGURATION (STRICT) ---
# Basierend auf deiner Audit-Liste. Jede QID hat einen festen Platz.
CATEGORY_CONFIG = {
    "Religione": {
        "color": "#E63946",
        "subgroups": {
            "Luoghi di Culto": {
                "Chiesa": ["Q16970", "Q317557"],
                "Chiesa parrocchiale": ["Q55876909", "Q317557", "Q1088552"],
                "Basilica minore": ["Q120560"],
                "Cappella": ["Q108325"],
                "Cappella cimiteriale": ["Q1457501"],
                "Oratorio": ["Q580499"],
                "Santuario": ["Q697295"],
                "Tempio": ["Q44539"],
                "Capitello votivo": ["Q3395121"],
                "Via Crucis": ["Q231685"],
                "Ex chiesa": ["Q19899465", "Q57644089", "Q96352496"]
            },
            "Vita Monastica": {
                "Monastero": ["Q44613"],
                "Abbazia": ["Q160742"],
                "Convento": ["Q1128397"],
                "Eremo": ["Q513550"],
                "Canonica": ["Q607241"]
            },
            "Cimiteri": {
                "Cimitero": ["Q39614"],
                "Cimitero di guerra": ["Q1241568"],
                "Necropoli": ["Q200141"],
                "Tomba": ["Q381885"],
                "Monumento funebre": ["Q56055312"]
            },
            "Amministrazione Religiosa": {
                "Parrocchia": ["Q102496"]
            }
        }
    },
    "Cultura e tempo libero": {
        "color": "#FFB703",
        "subgroups": {
            "Biblioteche": {
                "Biblioteca universitaria": ["Q1622062"],
                "Biblioteca pubblica": ["Q28564", "Q2326815", "Q124750618"],
                "Altre biblioteche": ["Q7075", "Q380829", "Q385994", "Q124750593", "Q124750711", "Q105763925", "Q1076099"] 
            },
            "Musei": {
                "Museo": ["Q33506"],
                "Museo d'arte": ["Q207694", "Q108860593"],
                "Pinacoteca": ["Q740437"],
                "Museo archeologico": ["Q3329412"],
                "Museo storico": ["Q16735822"],
                "Museo etnografico": ["Q12104174"],
                "Museo pubblico": ["Q124830213", "Q124830411"],
                "Museo didattico": ["Q94701740"],
                "Ecomuseo": ["Q94701721"],
                "Casa museo": ["Q2087181"]
            },
            "Archivi": {
                "Archivio": ["Q166118"],
                "Archivio comunale": ["Q604177"],
                "Archivio di Stato": ["Q17620767"],
                "Archivio accademico": ["Q27032435", "Q2877653"]
            },
            "Spettacolo": {
                "Teatro": ["Q24354"],
                "Teatro d'opera": ["Q153562"],
                "Cinema": ["Q41253"],
                "Casinò": ["Q133215"],
                "Centro culturale": ["Q1329623"]
            }
        }
    },
    "Amministrazione": {
        "color": "#F4A261",
        "subgroups": {
            "Istruzione": {
                "Scuola": ["Q3914"],
                "Scuola dell'infanzia": ["Q126807"],
                "Scuola primaria": ["Q9842"],
                "Istituto comprensivo": ["Q56177191"],
                "Conservatorio": ["Q184644"],
                "Complesso educativo": ["Q20860083"],
                "Istituzione accademica": ["Q4671277"]
            },
            "Uffici Pubblici": {
                "Municipio": ["Q25550691", "Q15303838"],
                "Comune italiano": ["Q747074"],
                "Palazzo di giustizia": ["Q1137809"],
                "Ufficio postale": ["Q35054"],
                "Agenzia governativa": ["Q1188075"],
                "Carcere": ["Q40357"]
            }
        }
    },
    "Fortificazioni e militare": {
        "color": "#606C38",
        "subgroups": {
            "Fortificazioni": {
                "Castello": ["Q23413"],
                "Castello in rovina": ["Q17715832"],
                "Rocca": ["Q1195705"],
                "Forte": ["Q1785071"],
                "Fortezza": ["Q57831"],
                "Fortificazione": ["Q57821"],
                "Mura cittadine": ["Q16748868"],
                "Porta cittadina": ["Q82117"],
                "Torre": ["Q12518"],
                "Torre campanaria": ["Q200334"],
                "Baluardo": ["Q81851"]
            },
            "Militare": {
                "Caserma": ["Q131263"],
                "Bunker": ["Q91122"],
                "Campo di concentramento": ["Q152081"]
            }
        }
    },
    "Natura e paesaggio": {
        "color": "#2A9D8F",
        "subgroups": {
            "Acqua": {
                "Lago": ["Q23397"],
                "Bacino artificiale": ["Q131681", "Q4735538"],
                "Fiume": ["Q4022"],
                "Sorgente": ["Q124714"],
                "Cascata": ["Q34038"]
            },
            "Mare e Costa": {
                "Spiaggia": ["Q40080"],
                "Baia": ["Q39594"],
                "Capo": ["Q185113"],
                "Isola": ["Q23442"]
            },
            "Montagna": {
                "Montagna": ["Q8502"],
                "Vetta": ["Q207326"],
                "Catena montuosa": ["Q46831"],
                "Valle": ["Q39816"],
                "Passo": ["Q2231510"],
                "Sella": ["Q10862618"],
                "Gap": ["Q16887036"],
                "Grotta": ["Q35509"]
            },
            "Verde": {
                "Parco": ["Q22698"],
                "Parco cittadino": ["Q22746"],
                "Giardino": ["Q1107656"],
                "Orto botanico": ["Q167346"],
                "Albero monumentale": ["Q811534"],
                "Area protetta": ["Q473972", "Q15069452", "Q3936950"]
            }
        }
    },
    "Dimore ed edifici": {
        "color": "#A53860",
        "subgroups": {
            "Residenze": {
                "Palazzo": ["Q16560"],
                "Palazzo italiano": ["Q2651004"],
                "Villa": ["Q3950", "Q80966", "Q111189432"],
                "Casa": ["Q3947"],
                "Casa rurale": ["Q16884952"],
                "Cascina": ["Q1169748"],
                "Grangia": ["Q1098590"],
                "Fattoria": ["Q1207909"]
            },
            "Ospitalità": {
                "Albergo": ["Q27686"],
                "Guest house": ["Q2460422"],
                "Rifugio di montagna": ["Q182676"],
                "Bivacco alpino": ["Q20743510"]
            },
            "Strutture Varie": {
                "Edificio": ["Q41176"],
                "Complesso di edifici": ["Q1497375"],
                "Struttura architettonica": ["Q811979"],
                "Rifugio per cani": ["Q1411287"],
                "Dépendance": ["Q3044808"]
            }
        }
    },
    "Insediamenti": {
        "color": "#457B9D",
        "subgroups": {
            "Centri": {
                "Frazione": ["Q1134686"],
                "Insediamento umano": ["Q486972"],
                "Località abitata": ["Q3835961"],
                "Cittadina": ["Q3957"],
                "Grande città": ["Q1549591"]
            },
            "Zone Urbane": {
                "Centro storico": ["Q676050"],
                "Quartiere": ["Q123705"],
                "Ghetto": ["Q152018"],
                "Piazza": ["Q174782"],
                "Piazza della cattedrale": ["Q131542697"]
            }
        }
    },
    "Infrastrutture": {
        "color": "#7D8597",
        "subgroups": {
            "Trasporti": {
                "Stazione ferroviaria": ["Q55488"],
                "Fermata ferroviaria": ["Q55678"],
                "Fermata dismessa": ["Q65464941"],
                "Stazione metropolitana": ["Q928830"],
                "Stazione sotterranea": ["Q22808403"],
                "Fermata tram": ["Q2175765"],
                "Linea ferroviaria": ["Q728937"],
                "Aeroporto": ["Q1248784"],
                "Aerodromo": ["Q94993988"],
                "Porto": ["Q44782"],
                "Valico di confine": ["Q55599109"],
                "Galleria": ["Q44377", "Q2354973"],
                "Strada urbana": ["Q79007"]
            },
            "Industria e Tecnica": {
                "Edificio industriale": ["Q1662011"],
                "Mulino": ["Q44494"],
                "Fabbrica": ["Q3973051"],
                "Centrale telefonica": ["Q256132"],
                "Centrale elettrica": ["Q339353"],
                "Lavatoio": ["Q1690211"],
                "Stazione meteorologica": ["Q190107"],
                "Società scientifica": ["Q955824"],
                "Osservatorio": ["Q1254933"]
            },
            "Opere Ingegneristiche": {
                "Ponte": ["Q12280"],
                "Viadotto": ["Q181348"],
                "Faro": ["Q39715"]
            },
            "Servizi": {
                "Servizio": ["Q13226383"],
                "Impresa sociale": ["Q1071015"],
                "Organizzazione no-profit": ["Q163740"]
            }
        }
    },
    "Archeologia": {
        "color": "#8D6E63",
        "subgroups": {
            "Siti": {
                "Sito archeologico": ["Q839954"],
                "Parco archeologico": ["Q3363945"],
                "Nuraghe": ["Q688292", "Q1385277"],
                "Domus de Janas": ["Q782970"],
                "Tomba dei giganti": ["Q1523627"],
                "Rovine": ["Q109607"],
                "Città antica": ["Q15661340"],
                "Anfiteatro": ["Q41735"],
                "Teatro romano": ["Q3243464"],
                "Terme": ["Q1341387"]
            }
        }
    },
    "Monumenti": {
        "color": "#9D4EDD",
        "subgroups": {
            "Monumenti": {
                "Monumento": ["Q4989906"],
                "Memoriale di guerra": ["Q575759"],
                "Targa commemorativa": ["Q721747"],
                "Arco di trionfo": ["Q200688"],
                "Statua": ["Q179700"],
                "Fontana": ["Q483453"],
                "Elemento architettonico": ["Q391414"]
            }
        }
    }
}

# --- GENERATE LOOKUP TABLE ---
QID_LOOKUP = {}
for group_name, group_data in CATEGORY_CONFIG.items():
    for subgroup_name, types_dict in group_data["subgroups"].items():
        for type_name, qids in types_dict.items():
            for qid in qids:
                QID_LOOKUP[qid] = {
                    "group": group_name,
                    "subgroup": subgroup_name,
                    "type": type_name
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
        headers = {'User-Agent': 'ItaliaWikidataCheck/11.0', 'Accept': 'text/csv'}
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

            # --- MATCHING LOGIC (3 LEVEL) ---
            if type_qid in QID_LOOKUP:
                match = QID_LOOKUP[type_qid]
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
        new_meta[key] = { "osm": current_osm_date, "wiki": now_str, "done": done, "total": total }

        with open(os.path.join(DATA_DIR, f"data_{key}.geojson"), 'w', encoding='utf-8') as f:
            json.dump({"type": "FeatureCollection", "features": features}, f)
        
        print(f"   -> Saved {total} items ({done} matched)")
        processed_count += 1
        time.sleep(1)

    if processed_count > 0:
        # Save Metadata
        with open(METADATA_FILE, 'w') as f:
            json.dump({ "global_osm_date": now_str, "global_wiki_date": now_str, "regions": new_meta }, f)
        
        # Save History
        history = []
        if os.path.exists(HISTORY_FILE):
            try:
                with open(HISTORY_FILE, 'r') as f: history = json.load(f)
            except: pass
        
        today_str = datetime.datetime.now().strftime("%Y-%m-%d")
        history = [h for h in history if h.get("date") != today_str]
        history.append({ "date": today_str, "data": new_meta })
        history.sort(key=lambda x: x['date'])
        
        with open(HISTORY_FILE, 'w') as f:
            json.dump(history, f)
            print(f"   -> History updated for {today_str}")

if __name__ == "__main__":
    main()
