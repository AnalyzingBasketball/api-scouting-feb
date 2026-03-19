import os
import requests
import pandas as pd
import numpy as np
import re
import json
import base64
import unicodedata
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# ==============================================================================
# 1. CONFIGURACIÓN Y RUTAS GLOBALES (RESTAURADA A dataTFM)
# ==============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "dataTFM")
REPORTS_DIR = os.path.join(DATA_DIR, "reports")

LOGO_EMPRESA = os.path.join(BASE_DIR, "images/logo.png")
LOGO_FEB = os.path.join(BASE_DIR, "images/feb.png")
LOGO_LIGA = os.path.join(BASE_DIR, "images/primera_feb.png")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)

HEADERS_WEB = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}
BASE_URL = "https://www.feb.es"

FILE_ROLES = os.path.join(DATA_DIR, "PLAYER_ROLES_FINAL_2526.csv")
FILE_LINEUPS = os.path.join(DATA_DIR, "LINEUPS_PRIMERAFEB_2526.csv")
FILE_CALENDAR = os.path.join(DATA_DIR, "calendario_maestro_primerafeb_2025.csv")
FILE_LOGOS = os.path.join(DATA_DIR, "logos_equipos.json")
FILE_PHOTOS = os.path.join(DATA_DIR, "raw_data", "PLAYER_NAMES_DICT.json")

# ==============================================================================
# CARGA EN MEMORIA (IDÉNTICO A SU CÓDIGO LOCAL)
# ==============================================================================
map_role_id = {}
map_role_name = {}
try:
    if os.path.exists(FILE_ROLES):
        df_roles_init = pd.read_csv(FILE_ROLES)
        for _, r in df_roles_init.iterrows():
            pid = str(r.get('PLAYER_ID', '')).strip()
            if pid.endswith('.0'): pid = pid[:-2]
            role = str(r.get('ROLE_NAME', 'N/A'))
            map_role_id[pid] = role
            
            pname = "".join([c for c in unicodedata.normalize('NFKD', str(r.get('PLAYER_NAME', ''))) if not unicodedata.combining(c)]).lower().strip()
            map_role_name[pname] = role
except: pass

map_role, map_pos, map_name, map_efg, map_ts, map_tov, map_orb, map_ftr, map_usg = {}, {}, {}, {}, {}, {}, {}, {}, {}
custom_photos = {}
dicc_logos = {}

def load_m14_mappings():
    global map_role, map_pos, map_name, map_efg, map_ts, map_tov, map_orb, map_ftr, map_usg, custom_photos, dicc_logos
    try:
        if os.path.exists(FILE_ROLES):
            df_roles = pd.read_csv(FILE_ROLES)
            TEAM_FIXES = {'CLUB OURENSE BALONCESTO': 'CLOUD.GAL OURENSE BALONCESTO', 'OURENSE BALONCESTO': 'CLOUD.GAL OURENSE BALONCESTO'}
            df_roles['TEAM'] = df_roles.get('TEAM', pd.Series()).replace(TEAM_FIXES)
            
            def safe_id_role(val):
                if pd.isna(val): return ""
                s = str(val).strip()
                return s[:-2] if s.endswith('.0') else s
                
            df_roles['PLAYER_ID'] = df_roles['PLAYER_ID'].apply(safe_id_role)
            map_role = df_roles.set_index('PLAYER_ID')['ROLE_NAME'].to_dict()
            map_pos = df_roles.set_index('PLAYER_ID')['POSITION'].to_dict()
            map_name = df_roles.set_index('PLAYER_ID')['PLAYER_NAME'].to_dict()
            if 'eFG%' in df_roles.columns: map_efg = df_roles.set_index('PLAYER_ID')['eFG%'].to_dict()
            if 'TS%' in df_roles.columns: map_ts = df_roles.set_index('PLAYER_ID')['TS%'].to_dict()
            if 'TOV%' in df_roles.columns: map_tov = df_roles.set_index('PLAYER_ID')['TOV%'].to_dict()
            if 'ORB%' in df_roles.columns: map_orb = df_roles.set_index('PLAYER_ID')['ORB%'].to_dict()
            if 'FTr' in df_roles.columns: map_ftr = df_roles.set_index('PLAYER_ID')['FTr'].to_dict()
            if 'USG%' in df_roles.columns: map_usg = df_roles.set_index('PLAYER_ID')['USG%'].to_dict()
    except: pass
    try:
        with open(FILE_PHOTOS, "r", encoding="utf-8") as f: custom_photos = json.load(f)
    except: custom_photos = {}
    try:
        with open(FILE_LOGOS, "r", encoding="utf-8") as f: dicc_logos = json.load(f)
    except: dicc_logos = {}

# ==============================================================================
# 2. FUNCIONES DE AYUDA GLOBALES
# ==============================================================================
def remove_accents(input_str):
    if pd.isna(input_str): return ""
    return "".join([c for c in unicodedata.normalize('NFKD', str(input_str)) if not unicodedata.combining(c)])

def limpiar_texto_archivo(texto):
    if not isinstance(texto, str): return "Desconocido"
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8')
    return re.sub(r'[^a-zA-Z0-9]', '', texto)

def clear_string(s):
    return ''.join(c for c in unicodedata.normalize('NFKD', str(s)) if not unicodedata.combining(c)).upper()

def safe_id(val):
    if pd.isna(val): return ""
    s = str(val).strip()
    if s.endswith('.0'): s = s[:-2]
    return s

def formatear_nombre_jugador(nombre):
    if not isinstance(nombre, str): return nombre
    partes = nombre.split('. ', 1)
    if len(partes) == 2: return f"{partes[0].upper()}. {partes[1].title()}"
    return nombre.title()

def get_short_name(full_name):
    if pd.isna(full_name) or full_name == 'Unknown': return "Unknown"
    partes = str(full_name).strip().split(" ")
    particulas = ['mc', 'mac', 'de', 'del', 'la', 'las', 'los', 'san', 'van', 'von', 'da', 'di']
    if len(partes) > 2 and partes[1].lower() in particulas:
        return " ".join(partes[:4]) if len(partes) > 3 and partes[2].lower() in particulas else " ".join(partes[:3])
    else: return " ".join(partes[:2]) if len(partes) >= 2 else full_name

def get_image_base64(path):
    try:
        with open(path, "rb") as img_file: return base64.b64encode(img_file.read()).decode('utf-8')
    except: return ""

def parse_min(val):
    if pd.isna(val): return 0
    if isinstance(val, str):
        if ':' in val: m, s = val.split(':'); return int(m)*60 + int(s)
        try: return float(val) * 60
        except: return 0
    return float(val) * 60

def safe_get(row, col_names, default=0):
    for c in col_names:
        if c in row.index and pd.notna(row[c]) and str(row[c]).strip() != "": return row[c]
    return default

def parse_shooting(row, prefix_made, prefix_att, prefix_combined):
    m = float(safe_get(row, prefix_made, 0))
    a = float(safe_get(row, prefix_att, 0))
    if m == 0 and a == 0:
        c = str(safe_get(row, prefix_combined, ""))
        if '/' in c:
            try: m = float(c.split('/')[0]); a = float(c.split('/')[1])
            except: pass
    return m, a

def clean_position(pos_text):
    if not pos_text: return ""
    t = remove_accents(pos_text).lower().replace("-", " ").replace(".", "").strip()
    if 'base' in t: return 'Base'
    if 'escolta' in t: return 'Escolta'
    if 'ala' in t and 'piv' in t: return 'Ala Pívot'
    if 'a piv' in t: return 'Ala Pívot'
    if 'alero' in t: return 'Alero'
    if 'piv' in t: return 'Pívot'
    return ""

def match_team_name(target_name, available_names):
    if not available_names: return target_name
    target_clean = remove_accents(str(target_name).lower())
    target_words = set(re.findall(r'\w+', target_clean))
    best_match = available_names[0]
    max_score = -1
    for cand in available_names:
        cand_clean = remove_accents(str(cand).lower())
        cand_words = set(re.findall(r'\w+', cand_clean))
        score = len(target_words.intersection(cand_words)) * 10
        if cand_clean in target_clean or target_clean in cand_clean: score += 5
        for cw in cand_words:
            for tw in target_words:
                if tw.startswith(cw) or cw.startswith(tw): score += 1
        if score > max_score:
            max_score = score
            best_match = cand
    return best_match

def get_classic_order(pid):
    load_m14_mappings()
    p_data = custom_photos.get(str(pid), {})
    pos_raw = p_data.get("POSITION", map_pos.get(str(pid), ""))
    order = p_data.get("POS_ORDER")
    if pd.notna(order) and str(order).strip() != "":
        try: return float(order)
        except: pass
    if pd.isna(pos_raw) or str(pos_raw).strip() == "": return 6
    pos_up = str(pos_raw).strip().upper()
    if 'PG' in pos_up or 'BASE' in pos_up: return 1
    if 'SG' in pos_up or 'ESCOLTA' in pos_up: return 2
    if 'SF' in pos_up or 'ALERO' in pos_up: return 3
    if 'PF' in pos_up or 'ALA' in pos_up: return 4
    if 'C' in pos_up or 'PIV' in pos_up: return 5
    return 6

def create_signatures(row):
    load_m14_mappings()
    players = [row['P1_ID'], row['P2_ID'], row['P3_ID'], row['P4_ID'], row['P5_ID']]
    roles = [map_role.get(p, "Unknown") for p in players]
    if "Unknown" in roles: return pd.Series(["Incomplete", "Incomplete"])
    roles.sort(); arch_sig = " / ".join(roles)
    players.sort(); real_sig = "-".join(players)
    return pd.Series([arch_sig, real_sig])

# ==============================================================================
# FUNCIONES DE EXTRACCIÓN (MÓDULO 12)
# ==============================================================================
def extraer_diccionario_logos():
    try:
        r = requests.get("https://www.feb.es/competiciones/calendario/primerafeb/1/2025", headers=HEADERS_WEB)
        soup = BeautifulSoup(r.text, 'html.parser')
        diccionario = {}
        for cont in soup.find_all("div", class_=lambda c: c and "contenedorLogoEquipoCalendario" in c):
            img, a = cont.find("img"), cont.find("a")
            if img and a:
                url_logo = img.get("src")
                if url_logo.startswith("/"): url_logo = "https://www.feb.es" + url_logo
                diccionario[a.text.strip()] = url_logo
        with open(FILE_LOGOS, "w", encoding="utf-8") as f:
            json.dump(diccionario, f, ensure_ascii=False, indent=4)
    except: pass

def construir_calendario_maestro():
    try:
        r = requests.get("https://www.feb.es/competiciones/calendario/primerafeb/1/2025", headers=HEADERS_WEB)
        soup = BeautifulSoup(r.text, 'html.parser')
        datos = []
        for col in soup.find_all('div', class_='columna'):
            h1 = col.find('h1', class_='titulo-modulo')
            if not h1: continue
            match_cab = re.search(r'(Jornada\s+\d+)\s+(.*)', h1.get_text(strip=True), re.IGNORECASE)
            jornada = match_cab.group(1) if match_cab else h1.get_text(strip=True)
            fecha = match_cab.group(2) if match_cab else ""
            tabla = col.find('table')
            if not tabla: continue
            for fila in tabla.find_all('tr'):
                if fila.find('th') or 'LOCAL' in fila.get_text(strip=True).upper(): continue
                a_eq = fila.find_all('a', href=re.compile(r'Equipo\.aspx'))
                a_p = fila.find('a', href=re.compile(r'Partido\.aspx\?p='))
                if a_p and len(a_eq) >= 2:
                    datos.append({
                        "match_id": re.search(r'p=(\d+)', a_p['href']).group(1),
                        "jornada": jornada, "fecha_jornada": fecha,
                        "equipo_local": a_eq[0].get_text(strip=True), "equipo_visitante": a_eq[-1].get_text(strip=True),
                        "resultado": a_p.get_text(strip=True)
                    })
        pd.DataFrame(datos).drop_duplicates(subset=['match_id']).to_csv(FILE_CALENDAR, index=False, encoding='utf-8-sig')
    except: pass

def extraer_maestro_jugadores():
    equipos = []
    try:
        r = requests.get("https://www.feb.es/primerafeb/equipos.aspx", headers=HEADERS_WEB)
        soup = BeautifulSoup(r.text, 'html.parser')
        for n in soup.find_all('div', class_='equipo'):
            a = n.find('a', href=True)
            if a: equipos.append((a.get_text(strip=True), a['href'] if a['href'].startswith('http') else BASE_URL + a['href']))
    except: return
    lista = []
    for nombre, url in equipos:
        try:
            rt = requests.get(url, headers=HEADERS_WEB)
            st = BeautifulSoup(rt.text, 'html.parser')
            for tbl in st.find_all('table'):
                for row in tbl.find_all('tr'):
                    cols = row.find_all('td')
                    if len(cols) >= 4:
                        texts = []
                        for c in cols:
                            text = c.get_text(strip=True)
                            if not text and c.find('img'):
                                img = c.find('img')
                                if img.has_attr('title') and img['title']: text = img['title'].strip()
                                elif img.has_attr('alt') and img['alt']: text = img['alt'].strip()
                            if text: texts.append(text)
                        n_raw, p_raw, nac_raw, alt = "", "", "-", None
                        for t in texts:
                            tl = remove_accents(t).lower()
                            if any(p in tl for p in ['base', 'escolta', 'alero', 'piv']): p_raw = t; continue
                            nums = re.findall(r'\b(1\d{2}|2\d{2})\b', t)
                            if (nums and 'cm' in tl) or (nums and len(t)<=4): alt = int(nums[0]); continue
                            if re.match(r'\d{2}/\d{2}/\d{4}', t) or (t.isdigit() and len(t)<=2) or "Nombre" in t: continue
                            if not n_raw and len(t)>=3: n_raw = t
                            elif n_raw and len(t)>=3: nac_raw = t
                        if not n_raw: continue
                        pos_limpia = clean_position(p_raw)
                        if not pos_limpia:
                            if alt:
                                if alt <= 190: pos_limpia = "Base"
                                elif alt <= 196: pos_limpia = "Escolta"
                                elif alt <= 200: pos_limpia = "Alero"
                                elif alt <= 204: pos_limpia = "Ala Pívot"
                                else: pos_limpia = "Pívot"
                            else: pos_limpia = "Alero"
                        particulas = ['mc', 'mac', 'de', 'del', 'la', 'las', 'los', 'san', 'van', 'von', 'da', 'di']
                        pts = n_raw.title().strip().split()
                        ini = pts[0][0].upper() + "."
                        is_foreign = nac_raw and 'españa' not in nac_raw.lower()
                        if is_foreign: short = f"{ini} {pts[-2]} {pts[-1]}" if len(pts)>=3 and pts[-2].lower() in particulas else f"{ini} {pts[-1]}"
                        else: short = f"{ini} {pts[1]} {pts[2]}" if len(pts)>=3 and pts[1].lower() in particulas else f"{ini} {pts[1]}" if len(pts)>1 else n_raw
                        lista.append({'Team': nombre.strip(), 'Player': n_raw.title(), 'Short_Name': short, 'Position': pos_limpia, 'Height_cm': alt, 'Nationality': nac_raw.title()})
        except: pass
    if lista: pd.DataFrame(lista).drop_duplicates(subset=['Player', 'Team']).to_csv(os.path.join(DATA_DIR, "maestro_jugadores_primerafeb.csv"), index=False, encoding='utf-8-sig')

def obtener_partidos_jornada(jornada_id):
    url_calendario = "https://www.feb.es/competiciones/calendario/primerafeb/1/2025" 
    res = requests.get(url_calendario, headers=HEADERS_WEB)
    soup = BeautifulSoup(res.text, 'html.parser')
    datos_partidos = []
    for col in soup.find_all('div', class_='columna'):
        h1 = col.find('h1', class_='titulo-modulo')
        if not h1: continue
        texto_cabecera = h1.get_text(strip=True)
        match_jornada = re.search(r'Jornada\s+(\d+)', texto_cabecera, re.IGNORECASE)
        if not match_jornada or str(match_jornada.group(1)) != str(jornada_id): continue
        
        fecha_partido = texto_cabecera.replace(match_jornada.group(0), "").strip()
        if not fecha_partido: fecha_partido = "Fecha Desconocida"

        tabla = col.find('table')
        if not tabla: continue
        for fila in tabla.find_all('tr'):
            if fila.find('th') or 'LOCAL' in fila.get_text(strip=True).upper(): continue
            enlaces_equipo = fila.find_all('a', href=re.compile(r'Equipo\.aspx', re.IGNORECASE))
            enlace_partido = fila.find('a', href=re.compile(r'Partido\.aspx\?p=', re.IGNORECASE))
            if enlace_partido and len(enlaces_equipo) >= 2:
                match_id = re.search(r'p=(\d+)', enlace_partido['href'], re.IGNORECASE).group(1)
                resultado = enlace_partido.get_text(strip=True)
                jugado = False
                if "-" in resultado and any(c.isdigit() for c in resultado): jugado = True
                datos_partidos.append({
                    "match_id": match_id, "equipo_local": enlaces_equipo[0].get_text(strip=True),
                    "equipo_visitante": enlaces_equipo[-1].get_text(strip=True), "resultado": resultado, "jugado": jugado,
                    "fecha": fecha_partido 
                })
    return datos_partidos

def extraer_partido_api(match_id):
    session = requests.Session()
    session.headers.update({"User-Agent": HEADERS_WEB['User-Agent'], "Origin": BASE_URL, "Referer": BASE_URL+"/", "Accept": "application/json"})
    try:
        url_web = f"https://www.feb.es/competiciones/partido/{match_id}"
        res_web = session.get(url_web)
        soup = BeautifulSoup(res_web.text, 'html.parser')
        token = soup.find('input', id='_ctl0_token')['value'].strip()
        session.headers.update({"Authorization": f"Bearer {token}"})
    except: return False

    base_url_api = "https://intrafeb.feb.es/LiveStats.API/api/v1"
    try:
        data_pbp = session.get(f"{base_url_api}/KeyFacts/{match_id}").json()
        pbp_list = data_pbp.get('PLAYBYPLAY', {}).get('LINES', [])
        pd.DataFrame(pbp_list).to_csv(os.path.join(DATA_DIR, f'pbp_{match_id}.csv'), index=False, encoding='utf-8-sig')

        data_box = session.get(f"{base_url_api}/BoxScore/{match_id}").json()
        teams_box = data_box.get('BOXSCORE', {}).get('TEAM', [])
        box_flat = []
        for t in teams_box:
            t_name = t.get('name')
            for p in t.get('PLAYER', []): p['team_name'] = t_name; box_flat.append(p)
        pd.DataFrame(box_flat).to_csv(os.path.join(DATA_DIR, f'boxscore_{match_id}.csv'), index=False, encoding='utf-8-sig')
        return True
    except: return False

def limpiar_y_avanzadas(match_id, local, visitante, jornada):
    jornada_str = f"Jornada-{jornada}"; local_str = limpiar_texto_archivo(local); visit_str = limpiar_texto_archivo(visitante)
    
    df_box = pd.read_csv(os.path.join(DATA_DIR, f"boxscore_{match_id}.csv"))
    mapeo = {'team_name': 'Team', 'no': 'No', 'inn': 'Starter', 'name': 'Player', 'minFormatted': 'Min', 'pts': 'PTS',
             'p2m': '2PM', 'p2a': '2PA', 'p2p': '2P%', 'p3m': '3PM', 'p3a': '3PA', 'p3p': '3P%', 'fgm': 'FGM', 'fga': 'FGA', 'fgp': 'FG%',
             'p1m': 'FTM', 'p1a': 'FTA', 'p1p': 'FT%', 'ro': 'OREB', 'rd': 'DREB', 'rt': 'TREB', 'assist': 'AST', 'to': 'TOV', 'st': 'STL',
             'bs': 'BLK', 'tc': 'BLKA', 'mt': 'DNK', 'pf': 'PF', 'rf': 'FD', 'pllss': '+/-', 'val': 'PIR', 'id': 'Player_ID', 'logo': 'Logo_URL', 'min': 'Min_Sec'}
    df_clean = df_box[[c for c in mapeo.keys() if c in df_box.columns]].rename(columns=mapeo)
    for col in ['2P%', '3P%', 'FG%', 'FT%']:
        if col in df_clean.columns: df_clean[col] = df_clean[col].astype(str).str.replace(',', '.').astype(float)
    if 'Player' in df_clean.columns: df_clean['Player'] = df_clean['Player'].apply(formatear_nombre_jugador)
    ruta_box_clean = os.path.join(DATA_DIR, f"boxscore_{match_id}_{jornada_str}_{local_str}_vs_{visit_str}_clean.csv")
    df_clean.to_csv(ruta_box_clean, index=False, encoding='utf-8-sig')

    df_pbp = pd.read_csv(os.path.join(DATA_DIR, f"pbp_{match_id}.csv"))
    df_pbp['Team'] = df_pbp['text'].str.extract(r'^\((.*?)\)')
    df_pbp['Player'] = df_pbp['text'].str.extract(r'^\(.*?\) (.*?):')
    df_pbp['Player'] = df_pbp['Player'].apply(formatear_nombre_jugador)
    map_acciones = {'assist': 'Assist', 'rebound': 'Rebound', 'recovery': 'Steal', 'lose': 'Turnover', 'foul': 'Foul', 'timeout': 'Timeout', 'blockshot': 'Block', 'period': 'Period'}
    df_pbp['Action_Type'] = df_pbp['action'].map(map_acciones)
    
    mask_shoot = df_pbp['action'] == 'shoot'
    valor_tiro = df_pbp.loc[mask_shoot, 'text'].str.extract(r'TIRO DE (\d)', expand=False)
    df_pbp.loc[mask_shoot & df_pbp['text'].str.contains('ANOTADO', na=False), 'Action_Type'] = valor_tiro + 'PT Made'
    df_pbp.loc[mask_shoot & df_pbp['text'].str.contains('FALLADO', na=False), 'Action_Type'] = valor_tiro + 'PT Missed'
    mask_ft = df_pbp['action'] == 'fthrow'
    df_pbp.loc[mask_ft & df_pbp['text'].str.contains('ANOTADO', na=False), 'Action_Type'] = 'FT Made'
    df_pbp.loc[mask_ft & df_pbp['text'].str.contains('FALLADO', na=False), 'Action_Type'] = 'FT Missed'
    df_pbp.loc[df_pbp['text'].str.contains('MATE ANOTADO', na=False), 'Action_Type'] = '2PT Made'
    
    mask_sub = df_pbp['action'] == 'subst'
    df_pbp.loc[mask_sub & df_pbp['text'].str.contains('Sale', na=False), 'Action_Type'] = 'Substitution Out'
    df_pbp.loc[mask_sub & df_pbp['text'].str.contains('Entra', na=False), 'Action_Type'] = 'Substitution In'

    df_pbp['scoreA_num'] = pd.to_numeric(df_pbp['scoreA'], errors='coerce')
    df_pbp['scoreB_num'] = pd.to_numeric(df_pbp['scoreB'], errors='coerce')
    df_pbp['sort_priority'] = 3
    df_pbp.loc[df_pbp['Action_Type'] == 'Substitution Out', 'sort_priority'] = 1
    df_pbp.loc[df_pbp['Action_Type'] == 'Substitution In', 'sort_priority'] = 2
    df_pbp = df_pbp.sort_values(by=['quarter', 'time', 'scoreA_num', 'scoreB_num', 'sort_priority'], ascending=[True, False, True, True, True]).reset_index(drop=True)
    
    time_td = pd.to_timedelta('00:' + df_pbp['time'])
    df_pbp['Seconds'] = time_td.dt.total_seconds().astype(int)
    df_pbp['scoreA'] = df_pbp['scoreA_num'].ffill().fillna(0).astype(int)
    df_pbp['scoreB'] = df_pbp['scoreB_num'].ffill().fillna(0).astype(int)

    home_teams = df_pbp[(df_pbp['scoreA'].diff() > 0) & df_pbp['Team'].notna()]['Team'].unique()
    box_teams = df_clean['Team'].unique()
    home_team = home_teams[0] if len(home_teams) > 0 else (box_teams[0] if len(box_teams) > 0 else None)
    away_team = box_teams[1] if len(box_teams) > 1 else (box_teams[0] if len(box_teams) > 0 else None)
    
    current_home = set(df_clean[(df_clean['Team'] == home_team) & (df_clean['Starter'] == 1)]['Player'].tolist())
    current_away = set(df_clean[(df_clean['Team'] == away_team) & (df_clean['Starter'] == 1)]['Player'].tolist())
    h1, h2, h3, h4, h5, a1, a2, a3, a4, a5 = [], [], [], [], [], [], [], [], [], []
    for idx, row in df_pbp.iterrows():
        if row['Action_Type'] == 'Substitution Out':
            if row['Team'] == home_team: current_home.discard(row['Player'])
            elif row['Team'] == away_team: current_away.discard(row['Player'])
        elif row['Action_Type'] == 'Substitution In':
            if row['Team'] == home_team: current_home.add(row['Player'])
            elif row['Team'] == away_team: current_away.add(row['Player'])
        ch_list = (sorted(list(current_home)) + [None]*5)[:5]; ca_list = (sorted(list(current_away)) + [None]*5)[:5]
        h1.append(ch_list[0]); h2.append(ch_list[1]); h3.append(ch_list[2]); h4.append(ch_list[3]); h5.append(ch_list[4])
        a1.append(ca_list[0]); a2.append(ca_list[1]); a3.append(ca_list[2]); a4.append(ca_list[3]); a5.append(ca_list[4])
    df_pbp['H1'], df_pbp['H2'], df_pbp['H3'], df_pbp['H4'], df_pbp['H5'] = h1, h2, h3, h4, h5
    df_pbp['A1'], df_pbp['A2'], df_pbp['A3'], df_pbp['A4'], df_pbp['A5'] = a1, a2, a3, a4, a5
    
    df_pbp = df_pbp.rename(columns={'quarter': 'Period', 'time': 'Time', 'scoreA': 'Score_Home', 'scoreB': 'Score_Away'})
    columnas_finales = ['Period', 'Time', 'Seconds', 'Score_Home', 'Score_Away', 'Team', 'Player', 'Action_Type', 'text', 'H1', 'H2', 'H3', 'H4', 'H5', 'A1', 'A2', 'A3', 'A4', 'A5']
    ruta_pbp_clean = os.path.join(DATA_DIR, f"pbp_{match_id}_{jornada_str}_{local_str}_vs_{visit_str}_clean.csv")
    df_pbp[[c for c in columnas_finales if c in df_pbp.columns]].to_csv(ruta_pbp_clean, index=False, encoding='utf-8-sig')

    return ruta_pbp_clean, ruta_box_clean

def limpiar_boxscore_api(match_id):
    df_box = pd.read_csv(os.path.join(DATA_DIR, f"boxscore_{match_id}.csv"))
    mapeo = {'team_name': 'Team', 'no': 'No', 'inn': 'Starter', 'name': 'Player', 'minFormatted': 'Min', 'pts': 'PTS',
             'p2m': '2PM', 'p2a': '2PA', 'p3m': '3PM', 'p3a': '3PA', 'fgm': 'FGM', 'fga': 'FGA',
             'p1m': 'FTM', 'p1a': 'FTA', 'ro': 'OREB', 'rd': 'DREB', 'rt': 'TREB', 'assist': 'AST', 'to': 'TOV', 'st': 'STL',
             'bs': 'BLK', 'tc': 'BLKA', 'mt': 'DNK', 'pf': 'PF', 'rf': 'FD', 'pllss': '+/-', 'val': 'PIR', 'id': 'Player_ID', 'logo': 'Logo_URL'}
    df_clean = df_box[[c for c in mapeo.keys() if c in df_box.columns]].rename(columns=mapeo)
    df_clean['Min_Sec_Num'] = df_clean['Min'].apply(parse_min)
    if 'Player' in df_clean.columns: df_clean['Player'] = df_clean['Player'].apply(formatear_nombre_jugador)
    return df_clean

# ==============================================================================
# RENDERIZADO HTML: MÓDULO 12 (QUINTETOS)
# ==============================================================================
def generar_html_quintetos(ruta_pbp_clean, ruta_box_clean, match_id, equipo_local, equipo_visit, fecha_partido):
    df_pbp = pd.read_csv(ruta_pbp_clean)
    df_box = pd.read_csv(ruta_box_clean)
    
    lista_maestro = []
    try:
        if os.path.exists(os.path.join(DATA_DIR, "maestro_jugadores_primerafeb.csv")):
            df_maestro = pd.read_csv(os.path.join(DATA_DIR, "maestro_jugadores_primerafeb.csv"))
            for _, r in df_maestro.iterrows(): lista_maestro.append({'name': str(r['Player']).strip().upper(), 'pos': str(r['Position']).strip()})
    except: pass

    dict_roles = {}
    for _, r in df_box.iterrows():
        pid = str(r.get('Player_ID', ''))
        if pid.endswith('.0'): pid = pid[:-2]
        pname_clean = remove_accents(str(r.get('Player', '')).strip().lower())
        dict_roles[pname_clean] = map_role_id.get(pid, map_role_name.get(pname_clean, "N/A"))

    def obtener_posicion_segura(box_name):
        if not lista_maestro: return "Alero"
        try:
            box_clean = remove_accents(box_name.upper()).replace('.', ''); box_parts = box_clean.split()
            if not box_parts: return "Alero"
            for m in lista_maestro:
                full_clean = remove_accents(m['name']).replace('.', ''); full_parts = full_clean.split()
                if not full_parts: continue
                if box_parts[0][0] == full_parts[0][0]: 
                    if all(part in full_parts or part in full_clean for part in box_parts[1:]): return m['pos']
            last_name = box_parts[-1]
            if len(last_name) > 3:
                posibles = [m for m in lista_maestro if last_name in remove_accents(m['name'])]
                if len(posibles) == 1: return posibles[0]['pos']
                elif len(posibles) > 1:
                    for p in posibles:
                        if any(f[0] == box_parts[0][0] for f in remove_accents(p['name']).split()): return p['pos']
                    return posibles[0]['pos']
            return "Alero"
        except: return "Alero"

    def get_escudo(eq_name):
        try:
            with open(FILE_LOGOS, "r", encoding="utf-8") as f: dicc = json.load(f)
            for k, v in dicc.items():
                if limpiar_texto_archivo(k).upper() == limpiar_texto_archivo(eq_name).upper(): return v
                if remove_accents(k).upper() in remove_accents(eq_name).upper() or remove_accents(eq_name).upper() in remove_accents(k).upper(): return v
        except: pass
        return "https://via.placeholder.com/80"
        
    escudo_local = get_escudo(equipo_local)
    escudo_visit = get_escudo(equipo_visit)

    dict_fotos = {}
    for _, r in df_box.iterrows():
        dict_fotos[remove_accents(str(r['Player']).strip().lower())] = r['Logo_URL']

    q_scores = []
    prev_h, prev_a = 0, 0
    for q in sorted(df_pbp['Period'].unique()):
        df_q = df_pbp[df_pbp['Period'] == q]
        if not df_q.empty:
            sh = int(df_q['Score_Home'].iloc[-1]); sa = int(df_q['Score_Away'].iloc[-1])
            q_scores.append(f"Q{q}: {sh - prev_h}-{sa - prev_a}")
            prev_h, prev_a = sh, sa
            
    score_home_final, score_away_final = int(df_pbp['Score_Home'].iloc[-1]), int(df_pbp['Score_Away'].iloc[-1])

    df_pbp = df_pbp.sort_values(['Period', 'Seconds'], ascending=[True, False]).reset_index(drop=True)
    df_pbp['Duration'] = 0.0
    for i in range(len(df_pbp)-1):
        if df_pbp.iloc[i]['Period'] == df_pbp.iloc[i+1]['Period']:
            diff = df_pbp.iloc[i]['Seconds'] - df_pbp.iloc[i+1]['Seconds']
            if diff > 0: df_pbp.at[i, 'Duration'] = diff

    df_pbp['Rebound_Type'] = 'DREB' 
    last_miss_team = None
    for i, row in df_pbp.iterrows():
        action, team = str(row['Action_Type']), row['Team']
        if 'Missed' in action: last_miss_team = team
        elif action == 'Rebound':
            df_pbp.at[i, 'Rebound_Type'] = 'OREB' if pd.notna(team) and last_miss_team == team else 'DREB'
            last_miss_team = None 
        elif 'Made' in action or 'Turnover' in action: last_miss_team = None 

    df_pbp['Lineup_Home'] = df_pbp[['H1', 'H2', 'H3', 'H4', 'H5']].apply(lambda x: ' | '.join(sorted([str(i) for i in x if pd.notna(i)])), axis=1)
    df_pbp['Lineup_Away'] = df_pbp[['A1', 'A2', 'A3', 'A4', 'A5']].apply(lambda x: ' | '.join(sorted([str(i) for i in x if pd.notna(i)])), axis=1)

    pbp_teams = df_pbp['Team'].dropna().unique().tolist()
    actual_local_pbp = match_team_name(equipo_local, pbp_teams)
    remaining_pbp = [t for t in pbp_teams if t != actual_local_pbp]
    actual_visit_pbp = match_team_name(equipo_visit, remaining_pbp) if remaining_pbp else (pbp_teams[1] if len(pbp_teams)>1 else actual_local_pbp)

    def calc_quintetos(col_agrup, eq_obj, eq_riv):
        datos = []
        for lineup, df_l in df_pbp.groupby(col_agrup):
            jugadores = lineup.split(' | ')
            if len(jugadores) < 1: continue 
            seg_jugados = df_l['Duration'].sum()
            acc_fav, acc_con = df_l[df_l['Team'] == eq_obj], df_l[df_l['Team'] == eq_riv]
            pts_fav = sum(int(re.search(r'\d', act).group()) if 'PT' in act else 1 for act in acc_fav[acc_fav['Action_Type'].str.contains('Made', na=False)]['Action_Type'] if re.search(r'\d', act) or 'FT' in act)
            pts_con = sum(int(re.search(r'\d', act).group()) if 'PT' in act else 1 for act in acc_con[acc_con['Action_Type'].str.contains('Made', na=False)]['Action_Type'] if re.search(r'\d', act) or 'FT' in act)
            
            if seg_jugados == 0 and pts_fav == 0 and pts_con == 0: continue

            mins, secs = divmod(int(seg_jugados), 60)
            fga_f, fta_f = len(acc_fav[acc_fav['Action_Type'].str.contains('PT', na=False)]), len(acc_fav[acc_fav['Action_Type'].str.contains('FT', na=False)])
            tov_f = len(acc_fav[acc_fav['Action_Type'] == 'Turnover'])
            poss_f = fga_f + (0.44 * fta_f) + tov_f or 1
            
            fga_c, fta_c = len(acc_con[acc_con['Action_Type'].str.contains('PT', na=False)]), len(acc_con[acc_con['Action_Type'].str.contains('FT', na=False)])
            tov_c = len(acc_con[acc_con['Action_Type'] == 'Turnover'])
            poss_c = fga_c + (0.44 * fta_c) + tov_c or 1

            pace = (((poss_f + poss_c) / 2) * 40 * 60) / seg_jugados if seg_jugados >= 30 else 0.0
            t2m, t3m = len(acc_fav[acc_fav['Action_Type'] == '2PT Made']), len(acc_fav[acc_fav['Action_Type'] == '3PT Made'])
            ast = len(acc_fav[acc_fav['Action_Type'] == 'Assist'])
            ast_to = ast / tov_f if tov_f > 0 else ast
            
            dreb, oreb = len(acc_fav[(acc_fav['Action_Type'] == 'Rebound') & (acc_fav['Rebound_Type'] == 'DREB')]), len(acc_fav[(acc_fav['Action_Type'] == 'Rebound') & (acc_fav['Rebound_Type'] == 'OREB')])
            opp_dreb, opp_oreb = len(acc_con[(acc_con['Action_Type'] == 'Rebound') & (acc_con['Rebound_Type'] == 'DREB')]), len(acc_con[(acc_con['Action_Type'] == 'Rebound') & (acc_con['Rebound_Type'] == 'OREB')])
            
            datos.append({
                'jugadores': jugadores, 'tiempo': f"{mins:02d}:{secs:02d}", 'segundos': seg_jugados,
                'pm': f"+{pts_fav - pts_con}" if pts_fav - pts_con > 0 else str(pts_fav - pts_con),
                'pts': pts_fav, 'pa': pts_con, 'dreb': dreb, 'oreb': oreb, 'opp_oreb': opp_oreb, 'ast': ast, 'tov': tov_f, 
                'ortg': f"{(pts_fav / poss_f) * 100:.1f}", 'drtg': f"{(pts_con / poss_c) * 100:.1f}", 
                'efg_pct': f"{((t2m + (1.5 * t3m)) / fga_f * 100) if fga_f > 0 else 0:.1f}%", 
                'ts_pct': f"{(pts_fav / (2 * (fga_f + 0.44 * fta_f)) * 100) if (fga_f + 0.44 * fta_f) > 0 else 0:.1f}%", 
                'orb_pct': f"{(oreb / (oreb + opp_dreb) * 100) if (oreb + opp_dreb) > 0 else 0:.1f}%", 
                'drb_pct': f"{(dreb / (dreb + opp_oreb) * 100) if (dreb + opp_oreb) > 0 else 0:.1f}%",
                'ast_to': f"{ast_to:.1f}", 'pace': f"{pace:.1f}"
            })
        return sorted(datos, key=lambda x: x['segundos'], reverse=True)

    lineups_local, lineups_visitante = calc_quintetos('Lineup_Home', actual_local_pbp, actual_visit_pbp), calc_quintetos('Lineup_Away', actual_visit_pbp, actual_local_pbp)

    orden_pos = {'Base': 1, 'Escolta': 2, 'Alero': 3, 'Ala Pívot': 4, 'Pívot': 5}
    def gen_filas(lineups_data):
        filas = ""
        particulas = ['mc', 'mac', 'de', 'del', 'la', 'las', 'los', 'san', 'van', 'von', 'da', 'di']
        for l in lineups_data:
            jugadores_ord = []
            for p in l['jugadores']:
                p_clean = remove_accents(p.strip().lower())
                f_url = dict_fotos.get(p_clean)
                if not f_url or pd.isna(f_url) or str(f_url).strip() in ["", "nan", "None"]:
                    for k, v in dict_fotos.items():
                        if p_clean in k or k in p_clean: f_url = v; break
                if not f_url or pd.isna(f_url) or str(f_url).strip() in ["", "nan", "None"]:
                    f_url = "https://via.placeholder.com/45/cbd5e0/ffffff?text=+"
                    
                pts = p.strip().split(" ")
                if len(pts) > 2 and pts[1].lower() in particulas: n_corto = " ".join(pts[:4]) if len(pts) > 3 and pts[2].lower() in particulas else " ".join(pts[:3])
                else: n_corto = " ".join(pts[:2]) if len(pts) >= 2 else p
                pos = obtener_posicion_segura(p); rank = orden_pos.get(pos, 6)
                
                role = dict_roles.get(p_clean, "N/A")
                html_tarjeta = f"<div class='player-card'><div style='color:#2b6cb0; font-size:10px; font-weight:900; margin-bottom:4px; text-transform:uppercase;'>{role}</div><img src='{f_url}'><br>{n_corto}<br><span class='player-pos'>{pos}</span></div>"
                jugadores_ord.append({'html': html_tarjeta, 'rank': rank})
            
            jugadores_ord.sort(key=lambda x: x['rank'])
            faces_html = "".join([j['html'] for j in jugadores_ord])
            pm_class = "pm-positive" if "+" in l['pm'] else ("pm-negative" if "-" in l['pm'] else "")
            
            filas += f"<tr><td class='lineups-cell'><div class='players-flex'>{faces_html}</div></td><td style='font-weight: bold;'>{l['tiempo']}</td><td class='{pm_class}' style='font-size: 17px;'>{l['pm']}</td><td style='font-weight: bold; color: #2b6cb0;'>{l['pts']}</td><td style='font-weight: bold; color: #e53e3e;'>{l['pa']}</td><td>{l['dreb']}</td><td style='color: #48bb78; font-weight: bold;'>{l['oreb']}</td><td style='color: #e53e3e; font-weight: bold;'>{l['opp_oreb']}</td><td>{l['ast']}</td><td style='color: #e53e3e; font-weight: bold;'>{l['tov']}</td><td style='font-weight: bold;'>{l['ortg']}</td><td style='font-weight: bold;'>{l['drtg']}</td><td>{l['efg_pct']}</td><td>{l['ts_pct']}</td><td>{l['orb_pct']}</td><td>{l['drb_pct']}</td><td>{l['ast_to']}</td><td style='font-weight: bold; color: #4a5568;'>{l['pace']}</td></tr>"
        return filas

    logo_empresa_b64, logo_feb_b64, logo_liga_b64 = get_image_base64(LOGO_EMPRESA), get_image_base64(LOGO_FEB), get_image_base64(LOGO_LIGA)
    html = f"""
    <!DOCTYPE html><html><head><meta charset="utf-8"><title>Lineups - {equipo_local} vs {equipo_visit}</title>
    <style>
        body {{ font-family: 'Segoe UI', sans-serif; background: #f4f6f9; color: #333; margin: 0; padding: 20px; padding-bottom: 80px; }}
        .header-container {{ background: #fff; padding: 20px 30px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 20px; }}
        .top-logos {{ display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #e2e8f0; padding-bottom: 15px; margin-bottom: 15px; }}
        .logo-side {{ height: 60px; max-width: 130px; object-fit: contain; }} .logo-center {{ height: 90px; max-width: 250px; object-fit: contain; }}
        .match-info {{ text-align: center; }} .team-score-block {{ display: flex; justify-content: center; align-items: center; gap: 20px; }}
        .team-shield {{ width: 80px; height: 80px; object-fit: contain; }} .team-score-block h1 {{ margin: 0; font-size: 32px; color: #1a202c; }}
        .scores {{ font-size: 14px; color: #718096; margin-top: 10px; font-weight: bold; }}
        .team-section-title {{ color: #2d3748; margin-top: 30px; border-left: 5px solid #2b6cb0; padding-left: 10px; }}
        .table-container {{ background: #fff; border-radius: 8px; overflow: hidden; margin-bottom: 20px; }}
        table {{ width: 100%; border-collapse: collapse; text-align: center; table-layout: fixed; }}
        th {{ background: #2d3748; color: #fff; padding: 14px 4px; font-size: 15px; }} 
        td {{ padding: 10px 4px; border-bottom: 1px solid #e2e8f0; font-size: 15px; }}
        th.lineups-col {{ width: 32%; text-align: left; padding-left: 15px; }} td.lineups-cell {{ text-align: left; }}
        .players-flex {{ display: flex; justify-content: flex-start; gap: 2px; padding-left: 5px; }}
        .player-card {{ text-align: center; font-size: 9px; width: 65px; font-weight: bold; color: #4a5568; overflow: hidden; }}
        .player-card img {{ width: 38px; height: 38px; border-radius: 50%; border: 2px solid #cbd5e0; object-fit: cover; }}
        .player-pos {{ font-size: 8px; color: #718096; font-weight: normal; text-transform: uppercase; }}
        .pm-positive {{ color: #48bb78; font-weight: bold; }} .pm-negative {{ color: #f56565; font-weight: bold; }}
        .legend-grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 20px; background: #fff; padding: 25px; border-radius: 12px; margin-top: 30px; margin-bottom: 40px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); }}
        .legend-item {{ font-size: 12px; color: #4a5568; line-height: 1.6; text-align: left; }} .legend-item b {{ color: #2d3748; }}
        .footer {{ position: fixed; bottom: 0; left: 0; width: 100%; background: #2d3748; color: #cbd5e0; text-align: center; padding: 15px 0; font-size: 14px; font-weight: 500; border-top: 4px solid #ed8936; z-index: 100; box-shadow: 0 -2px 10px rgba(0,0,0,0.2); }}
        .footer a {{ color: #fff; text-decoration: none; font-weight: bold; }}
    </style></head><body>
        <div class="header-container"><div class="top-logos"><img src="data:image/png;base64,{logo_feb_b64}" class="logo-side"><img src="data:image/png;base64,{logo_empresa_b64}" class="logo-center"><img src="data:image/png;base64,{logo_liga_b64}" class="logo-side"></div>
        <div class="match-info">
            <div class="team-score-block"><img src="{escudo_local}" class="team-shield"><h1>{equipo_local} {score_home_final} - {score_away_final} {equipo_visit}</h1><img src="{escudo_visit}" class="team-shield"></div>
            <div class="scores">{' | '.join(q_scores)}<br><span style="font-weight: normal; font-size: 13px; color: #a0aec0;">Match Date: {fecha_partido}</span></div>
        </div></div>
        <h2 class="team-section-title">{equipo_local}</h2><div class="table-container"><table><thead><tr><th class="lineups-col">LINEUPS</th><th>MIN</th><th>+/-</th><th>PTS</th><th>PA</th><th>DREB</th><th>OREB</th><th>OPP OREB</th><th>AST</th><th>TO</th><th>ORTG</th><th>DRTG</th><th>eFG%</th><th>TS%</th><th>ORB%</th><th>DRB%</th><th>AST/TO</th><th>PACE</th></tr></thead><tbody>{gen_filas(lineups_local)}</tbody></table></div>
        <h2 class="team-section-title">{equipo_visit}</h2><div class="table-container"><table><thead><tr><th class="lineups-col">LINEUPS</th><th>MIN</th><th>+/-</th><th>PTS</th><th>PA</th><th>DREB</th><th>OREB</th><th>OPP OREB</th><th>AST</th><th>TO</th><th>ORTG</th><th>DRTG</th><th>eFG%</th><th>TS%</th><th>ORB%</th><th>DRB%</th><th>AST/TO</th><th>PACE</th></tr></thead><tbody>{gen_filas(lineups_visitante)}</tbody></table></div>
        <div class="legend-grid"><div class="legend-item"><b>MIN:</b> Minutes played together.<br><b>+/-:</b> Plus/Minus point differential.<br><b>PTS / PA:</b> Points Scored / Allowed.<br><b>DREB / OREB:</b> Defensive / Offensive Rebounds.<br><b>OPP_OREB:</b> Opponent Offensive Rebounds.</div><div class="legend-item"><b>AST:</b> Assists.<br><b>TO:</b> Turnovers.<br><b>AST/TO:</b> Assist to Turnover Ratio.<br><b>ORTG:</b> Offensive Rating.<br><b>DRTG:</b> Defensive Rating.</div><div class="legend-item"><b>eFG%:</b> Effective Field Goal %.<br><b>TS%:</b> True Shooting %.<br><b>ORB%:</b> Offensive Rebound %.<br><b>DRB%:</b> Defensive Rebound %.<br><b>PACE:</b> Possessions per 40 min.</div></div>
        <div class="footer">© 2026 Analizing Basketball | <a href="https://www.analizingbasketball.com" target="_blank">www.analizingbasketball.com</a></div>
    </body></html>"""
    
    clean_local = limpiar_texto_archivo(equipo_local); clean_visit = limpiar_texto_archivo(equipo_visit)
    ruta_final = os.path.join(REPORTS_DIR, f"Lineup_Report_{match_id}_{clean_local}_vs_{clean_visit}.html")
    with open(ruta_final, "w", encoding="utf-8") as f: f.write(html)
    return ruta_final

# ==============================================================================
# RENDERIZADO HTML: MÓDULO 12 (BOXSCORE)
# ==============================================================================
def generar_html_boxscore(ruta_box_clean, ruta_pbp_clean, match_id, equipo_local, equipo_visit, fecha_partido):
    df_box = pd.read_csv(ruta_box_clean)
    df_pbp = pd.read_csv(ruta_pbp_clean)
    
    def get_escudo(eq_name):
        try:
            with open(FILE_LOGOS, "r", encoding="utf-8") as f: dicc = json.load(f)
            for k, v in dicc.items():
                if limpiar_texto_archivo(k).upper() == limpiar_texto_archivo(eq_name).upper(): return v
                if remove_accents(k).upper() in remove_accents(eq_name).upper() or remove_accents(eq_name).upper() in remove_accents(k).upper(): return v
        except: pass
        return "https://via.placeholder.com/80"
        
    escudo_local = get_escudo(equipo_local)
    escudo_visit = get_escudo(equipo_visit)

    q_scores = []
    prev_h, prev_a = 0, 0
    for q in sorted(df_pbp['Period'].unique()):
        df_q = df_pbp[df_pbp['Period'] == q]
        if not df_q.empty:
            sh = int(df_q['Score_Home'].iloc[-1]); sa = int(df_q['Score_Away'].iloc[-1])
            q_scores.append(f"Q{q}: {sh - prev_h}-{sa - prev_a}")
            prev_h, prev_a = sh, sa
            
    score_home_final, score_away_final = int(df_pbp['Score_Home'].iloc[-1]), int(df_pbp['Score_Away'].iloc[-1])

    box_teams = df_box['Team'].dropna().unique().tolist()
    actual_local_box = match_team_name(equipo_local, box_teams)
    remaining_box = [t for t in box_teams if t != actual_local_box]
    actual_visit_box = match_team_name(equipo_visit, remaining_box) if remaining_box else (box_teams[1] if len(box_teams)>1 else actual_local_box)

    team_mapping_box = {equipo_local: actual_local_box, equipo_visit: actual_visit_box}
    teams_data = {}
    particulas_apellidos = ['mc', 'mac', 'de', 'del', 'la', 'las', 'los', 'san', 'van', 'von', 'da', 'di']

    for team in [equipo_local, equipo_visit]:
        actual_team_name = team_mapping_box[team]
        t_df = df_box[df_box['Team'] == actual_team_name].copy()
        p_list = []
        t_tot = {'MIN_sec':0, 'PTS':0, 'PIR':0, 'FGM2':0, 'FGA2':0, 'FGM3':0, 'FGA3':0, 'FTM':0, 'FTA':0, 'ORB':0, 'DRB':0, 'TRB':0, 'AST':0, 'STL':0, 'TOV':0, 'BLK':0, 'PFD':0, 'PF':0}
        
        for _, row in t_df.iterrows():
            min_sec = parse_min(safe_get(row, ['Min']))
            pts = float(safe_get(row, ['PTS'])); pir = float(safe_get(row, ['PIR']))
            fg2m, fg2a = parse_shooting(row, ['2PM'], ['2PA'], ['2PT'])
            fg3m, fg3a = parse_shooting(row, ['3PM'], ['3PA'], ['3PT'])
            ftm, fta = parse_shooting(row, ['FTM'], ['FTA'], ['FT'])
            orb = float(safe_get(row, ['OREB'])); drb = float(safe_get(row, ['DREB'])); trb = float(safe_get(row, ['TREB']))
            ast = float(safe_get(row, ['AST'])); stl = float(safe_get(row, ['STL'])); tov = float(safe_get(row, ['TOV']))
            blk = float(safe_get(row, ['BLK'])); pfd = float(safe_get(row, ['FD'])); pf = float(safe_get(row, ['PF']))
            try: pm = int(float(str(safe_get(row, ['+/-'], 0)).replace('+', '')))
            except: pm = 0
            
            t_tot['MIN_sec']+=min_sec; t_tot['PTS']+=pts; t_tot['PIR']+=pir
            t_tot['FGM2']+=fg2m; t_tot['FGA2']+=fg2a; t_tot['FGM3']+=fg3m; t_tot['FGA3']+=fg3a
            t_tot['FTM']+=ftm; t_tot['FTA']+=fta; t_tot['ORB']+=orb; t_tot['DRB']+=drb; t_tot['TRB']+=trb
            t_tot['AST']+=ast; t_tot['STL']+=stl; t_tot['TOV']+=tov; t_tot['BLK']+=blk; t_tot['PFD']+=pfd; t_tot['PF']+=pf
            p_list.append({'row': row, 'min_sec': min_sec, 'pts': pts, 'pir': pir, 'pm': pm, 'fg2m': fg2m, 'fg2a': fg2a, 'fg3m': fg3m, 'fg3a': fg3a, 'ftm': ftm, 'fta': fta, 'orb': orb, 'drb': drb, 'trb': trb, 'ast': ast, 'stl': stl, 'tov': tov, 'blk': blk, 'pfd': pfd, 'pf': pf})
        teams_data[team] = {'players': p_list, 'totals': t_tot}

    html_tables = ""
    for team, opp_team in [(equipo_local, equipo_visit), (equipo_visit, equipo_local)]:
        t_data, opp_data = teams_data[team], teams_data[opp_team]
        t_tot, opp_tot = t_data['totals'], opp_data['totals']
        tm_MIN_sec = t_tot['MIN_sec'] if t_tot['MIN_sec'] > 0 else 200 * 60 * 5 
        tm_FGA = t_tot['FGA2'] + t_tot['FGA3']; tm_FGM = t_tot['FGM2'] + t_tot['FGM3']
        opp_FGA = opp_tot['FGA2'] + opp_tot['FGA3']
        tm_Poss = tm_FGA + 0.44 * t_tot['FTA'] + t_tot['TOV']
        opp_Poss = opp_FGA + 0.44 * opp_tot['FTA'] + opp_tot['TOV']

        html_tables += f"""<h2 class="team-section-title">{team}</h2><div class="table-container"><table><thead class="group-headers"><tr><th colspan="5" class="bg-info">INFO</th><th colspan="12" class="bg-trad">TRADITIONAL</th><th colspan="9" class="bg-shoot">SHOOTING</th><th colspan="15" class="bg-adv">ADVANCED METRICS</th></tr></thead><thead class="col-headers"><tr><th>PIC</th><th>PLAYER</th><th>ROLE</th><th>S</th><th>MIN</th><th>PTS</th><th>PIR</th><th>ORB</th><th>DRB</th><th>TRB</th><th>AST</th><th>STL</th><th>TOV</th><th>BLK</th><th>PFD</th><th>PF</th><th>+/-</th><th>2PM</th><th>2PA</th><th>2P%</th><th>3PM</th><th>3PA</th><th>3P%</th><th>FTM</th><th>FTA</th><th>FT%</th><th>GmSc</th><th>TS%</th><th>eFG%</th><th>3PAr</th><th>FTr</th><th>USG%</th><th>ORB%</th><th>DRB%</th><th>TRB%</th><th>AST%</th><th>STL%</th><th>BLK%</th><th>TOV%</th><th>PPP</th><th>PPS</th></tr></thead><tbody>"""
        
        tot_gmsc = 0
        for p in t_data['players']:
            row = p['row']
            player_raw = str(safe_get(row, ['Player'], 'Unknown'))
            partes = player_raw.strip().split(" ")
            if len(partes) > 2 and partes[1].lower() in particulas_apellidos: player = " ".join(partes[:4]) if len(partes) > 3 and partes[2].lower() in particulas_apellidos else " ".join(partes[:3])
            else: player = " ".join(partes[:2]) if len(partes) >= 2 else player_raw

            pid = str(safe_get(row, ['Player_ID'], ""))
            if pid.endswith('.0'): pid = pid[:-2]
            
            role = map_role_id.get(pid, map_role_name.get(remove_accents(player_raw.strip().lower()), "N/A"))

            foto = safe_get(row, ['Logo_URL'])
            if pd.isna(foto) or str(foto).strip() in ["", "nan", "None"]: foto = "https://via.placeholder.com/40/cbd5e0/ffffff?text=+"
            s_val = str(safe_get(row, ['Starter'], "")).strip().lower()
            s_str = "S" if s_val in ['1', 'true', 'yes', '*'] else ""
            
            mins, secs = divmod(int(p['min_sec']), 60)
            if p['min_sec'] == 0:
                html_tables += f"<tr><td class='td-info'><img src='{foto}' class='player-photo'></td><td class='td-info player-name'>{player}</td><td class='td-info font-bold text-blue' style='font-size:11px;'>{role}</td><td class='td-info font-bold' style='color:#2b6cb0;'>{s_str}</td><td class='td-info'><b>00:00</b></td><td colspan='36' class='td-trad text-center'>Did Not Play</td></tr>"
                continue
                
            pm_str = f"+{p['pm']}" if p['pm'] > 0 else str(p['pm'])
            pm_class = "text-green" if p['pm'] > 0 else ("text-red" if p['pm'] < 0 else "")
            pir_class = "text-green" if p['pir'] > 0 else ("text-red" if p['pir'] < 0 else "")
            
            fgm = p['fg2m'] + p['fg3m']; fga = p['fg2a'] + p['fg3a']
            ts_denom = 2 * (fga + 0.44 * p['fta'])
            ts_pct = (p['pts'] / ts_denom * 100) if ts_denom > 0 else 0
            efg_pct = ((fgm + 0.5 * p['fg3m']) / fga * 100) if fga > 0 else 0
            par3 = (p['fg3a'] / fga * 100) if fga > 0 else 0
            ftr = (p['fta'] / fga * 100) if fga > 0 else 0
            usg_pct = 100 * ((fga + 0.44 * p['fta'] + p['tov']) * (tm_MIN_sec / 5)) / (p['min_sec'] * tm_Poss) if tm_Poss > 0 else 0
            orb_pct = (100 * (p['orb'] * (tm_MIN_sec / 5)) / (p['min_sec'] * (t_tot['ORB'] + opp_tot['DRB']))) if (t_tot['ORB'] + opp_tot['DRB']) > 0 else 0
            drb_pct = (100 * (p['drb'] * (tm_MIN_sec / 5)) / (p['min_sec'] * (t_tot['DRB'] + opp_tot['ORB']))) if (t_tot['DRB'] + opp_tot['ORB']) > 0 else 0
            trb_pct = (100 * (p['trb'] * (tm_MIN_sec / 5)) / (p['min_sec'] * (t_tot['TRB'] + opp_tot['TRB']))) if (t_tot['TRB'] + opp_tot['TRB']) > 0 else 0
            ast_denom = (((p['min_sec'] / (tm_MIN_sec / 5)) * tm_FGM) - fgm) if tm_MIN_sec > 0 else 0
            ast_pct = (100 * p['ast'] / ast_denom) if ast_denom > 0 else 0
            stl_pct = (100 * (p['stl'] * (tm_MIN_sec / 5)) / (p['min_sec'] * opp_Poss)) if opp_Poss > 0 else 0
            blk_pct = (100 * (p['blk'] * (tm_MIN_sec / 5)) / (p['min_sec'] * opp_tot['FGA2'])) if opp_tot['FGA2'] > 0 else 0
            tov_denom = fga + 0.44 * p['fta'] + p['tov']
            tov_pct = (100 * p['tov'] / tov_denom) if tov_denom > 0 else 0
            gmsc = p['pts'] + 0.4 * fgm - 0.7 * fga - 0.4 * (p['fta'] - p['ftm']) + 0.7 * p['orb'] + 0.3 * p['drb'] + p['stl'] + 0.7 * p['ast'] + 0.7 * p['blk'] - 0.4 * p['pf'] - p['tov']
            tot_gmsc += gmsc
            ppp = (p['pts'] / tov_denom) if tov_denom > 0 else 0
            pps = (p['pts'] / fga) if fga > 0 else 0
            fg2_pct = (p['fg2m']/p['fg2a']*100) if p['fg2a'] > 0 else 0
            fg3_pct = (p['fg3m']/p['fg3a']*100) if p['fg3a'] > 0 else 0
            ft_pct = (p['ftm']/p['fta']*100) if p['fta'] > 0 else 0
            
            html_tables += f"<tr><td class='td-info'><img src='{foto}' class='player-photo'></td><td class='td-info player-name'>{player}</td><td class='td-info font-bold text-blue' style='font-size:11px;'>{role}</td><td class='td-info font-bold' style='color:#2b6cb0;'>{s_str}</td><td class='td-info'><b>{mins:02d}:{secs:02d}</b></td><td class='td-trad font-bold text-blue'>{int(p['pts'])}</td><td class='td-trad font-bold {pir_class}'>{int(p['pir'])}</td><td class='td-trad'>{int(p['orb'])}</td><td class='td-trad'>{int(p['drb'])}</td><td class='td-trad font-bold'>{int(p['trb'])}</td><td class='td-trad'>{int(p['ast'])}</td><td class='td-trad text-green'>{int(p['stl'])}</td><td class='td-trad text-red'>{int(p['tov'])}</td><td class='td-trad'>{int(p['blk'])}</td><td class='td-trad text-gray'>{int(p['pfd'])}</td><td class='td-trad text-gray'>{int(p['pf'])}</td><td class='td-trad font-bold {pm_class}'>{pm_str}</td><td class='td-shoot font-bold'>{int(p['fg2m'])}</td><td class='td-shoot text-gray'>{int(p['fg2a'])}</td><td class='td-shoot'>{fg2_pct:.0f}%</td><td class='td-shoot font-bold'>{int(p['fg3m'])}</td><td class='td-shoot text-gray'>{int(p['fg3a'])}</td><td class='td-shoot'>{fg3_pct:.0f}%</td><td class='td-shoot font-bold'>{int(p['ftm'])}</td><td class='td-shoot text-gray'>{int(p['fta'])}</td><td class='td-shoot'>{ft_pct:.0f}%</td><td class='td-adv font-bold'>{gmsc:.1f}</td><td class='td-adv'>{ts_pct:.1f}%</td><td class='td-adv'>{efg_pct:.1f}%</td><td class='td-adv text-gray'>{par3:.1f}%</td><td class='td-adv text-gray'>{ftr:.1f}%</td><td class='td-adv font-bold text-blue'>{usg_pct:.1f}%</td><td class='td-adv'>{orb_pct:.1f}%</td><td class='td-adv'>{drb_pct:.1f}%</td><td class='td-adv text-gray'>{trb_pct:.1f}%</td><td class='td-adv'>{ast_pct:.1f}%</td><td class='td-adv'>{stl_pct:.1f}%</td><td class='td-adv'>{blk_pct:.1f}%</td><td class='td-adv'>{tov_pct:.1f}%</td><td class='td-adv font-bold'>{ppp:.2f}</td><td class='td-adv font-bold'>{pps:.2f}</td></tr>"
            
        tm_mins, tm_secs = divmod(int(tm_MIN_sec / 5), 60)
        tm_ts_denom = 2 * (tm_FGA + 0.44 * t_tot['FTA'])
        html_tables += f"<tr class='total-row'><td colspan='4' class='td-info' style='text-align: right; padding-right: 15px;'><b>TEAM TOTALS</b></td><td class='td-info'><b>{tm_mins:02d}:{tm_secs:02d}</b></td><td class='td-trad font-bold text-blue'>{int(t_tot['PTS'])}</td><td class='td-trad font-bold'>{int(t_tot['PIR'])}</td><td class='td-trad'>{int(t_tot['ORB'])}</td><td class='td-trad'>{int(t_tot['DRB'])}</td><td class='td-trad font-bold'>{int(t_tot['TRB'])}</td><td class='td-trad'>{int(t_tot['AST'])}</td><td class='td-trad text-green'>{int(t_tot['STL'])}</td><td class='td-trad text-red'>{int(t_tot['TOV'])}</td><td class='td-trad'>{int(t_tot['BLK'])}</td><td class='td-trad text-gray'>{int(t_tot['PFD'])}</td><td class='td-trad text-gray'>{int(t_tot['PF'])}</td><td class='td-trad'></td><td class='td-shoot font-bold'>{int(t_tot['FGM2'])}</td><td class='td-shoot text-gray'>{int(t_tot['FGA2'])}</td><td class='td-shoot'>{(t_tot['FGM2']/t_tot['FGA2']*100) if t_tot['FGA2']>0 else 0:.0f}%</td><td class='td-shoot font-bold'>{int(t_tot['FGM3'])}</td><td class='td-shoot text-gray'>{int(t_tot['FGA3'])}</td><td class='td-shoot'>{(t_tot['FGM3']/t_tot['FGA3']*100) if t_tot['FGA3']>0 else 0:.0f}%</td><td class='td-shoot font-bold'>{int(t_tot['FTM'])}</td><td class='td-shoot text-gray'>{int(t_tot['FTA'])}</td><td class='td-shoot'>{(t_tot['FTM']/t_tot['FTA']*100) if t_tot['FTA']>0 else 0:.0f}%</td><td class='td-adv font-bold'>{tot_gmsc:.1f}</td><td class='td-adv'>{(t_tot['PTS']/tm_ts_denom*100) if tm_ts_denom>0 else 0:.1f}%</td><td class='td-adv'>{((tm_FGM + 0.5 * t_tot['FGM3'])/tm_FGA*100) if tm_FGA>0 else 0:.1f}%</td><td class='td-adv text-gray'>{(t_tot['FGA3']/tm_FGA*100) if tm_FGA>0 else 0:.1f}%</td><td class='td-adv text-gray'>{(t_tot['FTA']/tm_FGA*100) if tm_FGA>0 else 0:.1f}%</td><td class='td-adv font-bold text-blue'>100.0%</td><td class='td-adv'>{(100*t_tot['ORB']/(t_tot['ORB']+opp_tot['DRB'])) if (t_tot['ORB']+opp_tot['DRB'])>0 else 0:.1f}%</td><td class='td-adv'>{(100*t_tot['DRB']/(t_tot['DRB']+opp_tot['ORB'])) if (t_tot['DRB']+opp_tot['ORB'])>0 else 0:.1f}%</td><td class='td-adv text-gray'>{(100*t_tot['TRB']/(t_tot['TRB']+opp_tot['TRB'])) if (t_tot['TRB']+opp_tot['TRB'])>0 else 0:.1f}%</td><td class='td-adv'>{(100*t_tot['AST']/tm_FGM) if tm_FGM>0 else 0:.1f}%</td><td class='td-adv'>{(100*t_tot['STL']/opp_Poss) if opp_Poss>0 else 0:.1f}%</td><td class='td-adv'>{(100*t_tot['BLK']/opp_tot['FGA2']) if opp_tot['FGA2']>0 else 0:.1f}%</td><td class='td-adv'>{(100*t_tot['TOV']/(tm_FGA+0.44*t_tot['FTA']+t_tot['TOV'])) if (tm_FGA+0.44*t_tot['FTA']+t_tot['TOV'])>0 else 0:.1f}%</td><td class='td-adv font-bold'>{(t_tot['PTS']/(tm_FGA+0.44*t_tot['FTA']+t_tot['TOV'])) if (tm_FGA+0.44*t_tot['FTA']+t_tot['TOV'])>0 else 0:.2f}</td><td class='td-adv font-bold'>{(t_tot['PTS']/tm_FGA) if tm_FGA>0 else 0:.2f}</td></tr></tbody></table></div>"

    logo_empresa_b64, logo_feb_b64, logo_liga_b64 = get_image_base64(LOGO_EMPRESA), get_image_base64(LOGO_FEB), get_image_base64(LOGO_LIGA)
    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Advanced Boxscore - {equipo_local} vs {equipo_visit}</title>
    <style>
        body {{ font-family: 'Segoe UI', sans-serif; background: #f4f6f9; color: #1a202c; margin: 0; padding: 20px; padding-bottom: 80px; }}
        .header-container {{ background: #fff; padding: 20px 30px; border-radius: 12px; margin-bottom: 25px; }}
        .top-logos {{ display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #edf2f7; padding-bottom: 15px; margin-bottom: 20px; }}
        .logo-side {{ height: 60px; max-width: 130px; object-fit: contain; }} .logo-center {{ height: 90px; max-width: 250px; object-fit: contain; }}
        .team-score-block {{ display: flex; justify-content: center; align-items: center; gap: 30px; }} .team-shield {{ width: 90px; height: 90px; object-fit: contain; }}
        .team-score-block h1 {{ margin: 0; font-size: 36px; color: #2d3748; }} .scores {{ font-size: 15px; color: #a0aec0; margin-top: 15px; font-weight: bold; text-align: center; }}
        .team-section-title {{ color: #fff; background: #2d3748; padding: 10px 20px; border-radius: 8px; margin-top: 40px; margin-bottom: 15px; font-size: 22px; }}
        .table-container {{ background: #fff; border-radius: 12px; overflow-x: auto; margin-bottom: 25px; }}
        table {{ width: 100%; border-collapse: collapse; text-align: center; white-space: nowrap; }}
        .bg-info {{ background: #2d3748 !important; color: #fff !important; }} .bg-trad {{ background: #4a5568 !important; color: #fff !important; }} .bg-shoot {{ background: #2b6cb0 !important; color: #fff !important; }} .bg-adv {{ background: #2c7a7b !important; color: #fff !important; }}
        .col-headers th {{ background: #edf2f7; color: #4a5568; font-size: 11px; padding: 10px 5px; border-bottom: 2px solid #cbd5e0; }}
        td {{ padding: 8px 4px; font-size: 12px; border-bottom: 1px solid #edf2f7; vertical-align: middle; }}
        .td-info {{ background: #ffffff; }} .td-trad {{ background: #f8fafc; }} .td-shoot {{ background: #ebf8fa; }} .td-adv {{ background: #f0fff4; }}
        .total-row td {{ background: #e2e8f0; font-weight: bold; border-top: 2px solid #a0aec0; }}
        .player-name {{ text-align: left; font-weight: 700; color: #2d3748; font-size: 13px; }} .player-photo {{ width: 36px; height: 36px; border-radius: 50%; border: 2px solid #cbd5e0; object-fit: cover; }}
        .text-blue {{ color: #2b6cb0; }} .text-red {{ color: #e53e3e; }} .text-green {{ color: #38a169; }} .text-gray {{ color: #a0aec0; }} .font-bold {{ font-weight: bold; }}
        .legend-grid {{ display: grid; grid-template-columns: repeat(5, 1fr); gap: 15px; background: #fff; padding: 25px; border-radius: 12px; margin-top: 30px; margin-bottom: 40px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); }}
        .legend-item {{ font-size: 11px; color: #4a5568; line-height: 1.6; text-align: left; }} .legend-item b {{ color: #2d3748; }}
        .footer {{ position: fixed; bottom: 0; left: 0; width: 100%; background: #2d3748; color: #cbd5e0; text-align: center; padding: 15px 0; font-size: 14px; font-weight: 500; border-top: 4px solid #ed8936; z-index: 100; box-shadow: 0 -2px 10px rgba(0,0,0,0.2); }}
        .footer a {{ color: #fff; text-decoration: none; font-weight: bold; }}
    </style></head><body>
        <div class="header-container"><div class="top-logos"><img src="data:image/png;base64,{logo_feb_b64}" class="logo-side"><img src="data:image/png;base64,{logo_empresa_b64}" class="logo-center"><img src="data:image/png;base64,{logo_liga_b64}" class="logo-side"></div>
        <div class="match-info">
            <div class="team-score-block"><img src="{escudo_local}" class="team-shield"><h1>{equipo_local} {score_home_final} - {score_away_final} {equipo_visit}</h1><img src="{escudo_visit}" class="team-shield"></div>
            <div class="scores">{' | '.join(q_scores)}<br><span style="font-weight: normal; font-size: 13px; color: #a0aec0;">Match Date: {fecha_partido}</span></div>
        </div></div>
        {html_tables}
        <div class="legend-grid"><div class="legend-item"><b>PIC / PLAYER:</b> Player Info.<br><b>S:</b> Starter Player.<br><b>MIN:</b> Minutes Played.<br><b>PTS:</b> Points Scored.<br><b>PIR:</b> Performance Index Rating.<br><b>+/-:</b> Plus/Minus point differential.</div><div class="legend-item"><b>ORB:</b> Offensive Rebounds.<br><b>DRB:</b> Defensive Rebounds.<br><b>TRB:</b> Total Rebounds.<br><b>AST:</b> Assists.<br><b>STL:</b> Steals.<br><b>TOV:</b> Turnovers.</div><div class="legend-item"><b>BLK:</b> Blocks.<br><b>PFD:</b> Personal Fouls Drawn.<br><b>PF:</b> Personal Fouls Committed.<br><b>2PM/A:</b> 2-Point Goals Made/Attempted.<br><b>3PM/A:</b> 3-Point Goals Made/Attempted.<br><b>FTM/A:</b> Free Throws Made/Attempted.</div><div class="legend-item"><b>GmSc:</b> Game Score (Productivity metric).<br><b>TS%:</b> True Shooting Percentage.<br><b>eFG%:</b> Effective Field Goal Percentage.<br><b>3PAr:</b> 3-Point Attempt Rate.<br><b>FTr:</b> Free Throw Attempt Rate.<br><b>USG%:</b> Usage Percentage.</div><div class="legend-item"><b>ORB% / DRB% / TRB%:</b> Rebound Percentages.<br><b>AST%:</b> Assist Percentage.<br><b>STL% / BLK%:</b> Steal & Block Percentages.<br><b>TOV%:</b> Turnover Percentage.<br><b>PPP:</b> Points Per Possession.<br><b>PPS:</b> Points Per Shot.</div></div>
        <div class="footer">© 2026 Analizing Basketball | <a href="https://www.analizingbasketball.com" target="_blank">www.analizingbasketball.com</a></div>
    </body></html>
    """
    clean_local = limpiar_texto_archivo(equipo_local); clean_visit = limpiar_texto_archivo(equipo_visit)
    ruta_final = os.path.join(REPORTS_DIR, f"Boxscore_{match_id}_{clean_local}_vs_{clean_visit}.html")
    with open(ruta_final, "w", encoding="utf-8") as f: f.write(html)
    return ruta_final

# ==============================================================================
# RENDERIZADO HTML: MÓDULO 14 (CONTEXTUAL)
# ==============================================================================
def HTML_LINEUPS_AGREGADOS(efficiency, eq, context_str, m_filt):
    load_m14_mappings()
    def render_table(df_subset):
        t_html = f"""<div class='table-container'><table><thead><tr>
            <th class='col-lineup'></th><th>TOTAL MIN</th><th>PTS /40</th><th>PA /40</th><th>NET RTG /40</th>
            <th style='background:#2c7a7b'>TS% *</th><th style='background:#2c7a7b'>eFG% *</th><th style='background:#2c7a7b'>TOV% *</th><th style='background:#2c7a7b'>ORB% *</th><th style='background:#2c7a7b'>FTr *</th><th style='background:#2c7a7b'>USG% *</th>
            </tr></thead><tbody>"""
        for _, row in df_subset.iterrows():
            pm_val = row['NET_RATING']
            color_class = "text-green" if pm_val > 0 else ("text-red" if pm_val < 0 else "")
            sign = "+" if pm_val > 0 else ""
            p_ids = row['REAL_LINEUP'].split("-")
            p_ids.sort(key=get_classic_order)
            
            cards_html = ""
            avg_efg, avg_ts, avg_tov, avg_orb, avg_ftr, avg_usg, count = 0, 0, 0, 0, 0, 0, 0
            for pid in p_ids:
                p_data = custom_photos.get(pid, {})
                name_short = get_short_name(p_data.get("PLAYER_NAME", map_name.get(pid, "Unknown")))
                pos = p_data.get("POSITION", map_pos.get(pid, "N/A"))
                role = map_role.get(pid, "Unknown Role")
                foto_url = p_data.get("PHOTO_URL", f"https://imagenes.feb.es/Foto.aspx?c={pid}")
                cards_html += f"""<div class='player-card'><span class='player-role-label'>{role}</span><img src='{foto_url}' onerror="this.src='https://via.placeholder.com/50/cbd5e0/ffffff?text=+'"><br>{name_short}<br><span class='player-pos'>{pos}</span></div>"""
                if pid in map_efg:
                    avg_efg += map_efg.get(pid, 0); avg_ts += map_ts.get(pid, 0); avg_tov += map_tov.get(pid, 0)
                    avg_orb += map_orb.get(pid, 0); avg_ftr += map_ftr.get(pid, 0); avg_usg += map_usg.get(pid, 0); count += 1
            
            f_efg = f"{(avg_efg/count):.1f}%" if count > 0 else "N/A"
            f_ts = f"{(avg_ts/count):.1f}%" if count > 0 else "N/A"
            f_tov = f"{(avg_tov/count):.1f}%" if count > 0 else "N/A"
            f_orb = f"{(avg_orb/count):.1f}%" if count > 0 else "N/A"
            f_ftr = f"{(avg_ftr/count):.3f}" if count > 0 else "N/A"
            f_usg = f"{(avg_usg/count):.1f}%" if count > 0 else "N/A"
            
            t_html += f"""<tr><td style='text-align:left; padding-left:15px;'><div class='players-flex'>{cards_html}</div></td><td class='metric-big'>{row['MINUTES']:.1f}</td><td class='metric-big text-blue'>{row['PTS_40']:.1f}</td><td class='metric-big text-red'>{row['PA_40']:.1f}</td><td class='metric-huge {color_class}'>{sign}{pm_val}</td><td class='metric-adv' style='color:#B22222'>{f_ts}</td><td class='metric-adv'>{f_efg}</td><td class='metric-adv'>{f_tov}</td><td class='metric-adv'>{f_orb}</td><td class='metric-adv'>{f_ftr}</td><td class='metric-adv'>{f_usg}</td></tr>"""
        t_html += "</tbody></table></div>"
        return t_html

    df_equipo = efficiency.sort_values(by='NET_RATING', ascending=False)
    top3 = df_equipo.head(3); bottom3 = df_equipo.loc[~df_equipo.index.isin(top3.index)].tail(3) if len(df_equipo) > 3 else pd.DataFrame()

    eq_clean = clear_string(eq); logo_url = "https://via.placeholder.com/60"
    for k, v in dicc_logos.items():
        if clear_string(k) == eq_clean or eq_clean in clear_string(k): logo_url = v; break

    html_content = f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Contextual Scouting Lineups</title>
    <style>
        body {{ font-family: 'Segoe UI', sans-serif; background: #f4f6f9; color: #333; margin: 0; padding: 20px; padding-bottom: 80px; }}
        .top-banner {{ background: #fff; padding: 20px 30px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 20px; }}
        .top-logos {{ display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #e2e8f0; padding-bottom: 15px; margin-bottom: 15px; }}
        .logo-side {{ height: 60px; max-width: 130px; object-fit: contain; }} .logo-center {{ height: 90px; max-width: 250px; object-fit: contain; }}
        .header-title-block {{ text-align: center; }} h1 {{ margin: 0; font-size: 32px; color: #1a202c; text-transform: uppercase; }}
        .subtitle {{ color: #d69e2e; font-size: 18px; margin-top: 10px; font-weight: bold; background:#fffff0; display:inline-block; padding:5px 15px; border-radius:20px; border:1px solid #f6e05e; }}
        .legend-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 20px; background: #fff; padding: 20px; border-radius: 8px; border: 1px solid #e2e8f0; margin-bottom: 40px; }}
        .legend-item {{ font-size: 12px; color: #4a5568; line-height: 1.6; text-align: left; }} .legend-item b {{ color: #2d3748; }}
        .team-section {{ background: #fff; border-radius: 12px; padding: 20px; margin-bottom: 40px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); border: 1px solid #e2e8f0; }}
        .team-title-block {{ display: flex; align-items: center; gap: 15px; border-bottom: 3px solid #2b6cb0; padding-bottom: 10px; margin-bottom: 20px; }}
        .team-shield {{ width: 60px; height: 60px; object-fit: contain; }} h2 {{ margin: 0; font-size: 28px; color: #2d3748; text-transform: uppercase; font-weight: 800; }}
        .table-title {{ font-size: 18px; font-weight: bold; margin-bottom: 10px; padding-left: 10px; border-left: 5px solid; text-transform: uppercase; }}
        .title-top {{ border-color: #2b6cb0; color: #2b6cb0; }} .title-bot {{ border-color: #718096; color: #4a5568; }}
        .table-container {{ overflow-x: auto; margin-bottom: 30px; }} table {{ width: 100%; min-width: 1100px; border-collapse: collapse; text-align: center; table-layout: fixed; }}
        th {{ background: #2d3748; color: #fff; padding: 18px 6px; font-size: 16px; font-weight: 800; text-transform: uppercase; }} td {{ padding: 14px 4px; border-bottom: 1px solid #e2e8f0; vertical-align: middle; font-size: 14px; font-weight: 600; color: #2d3748; }}
        th.col-lineup {{ width: 40%; text-align: left; padding-left: 15px; }} .players-flex {{ display: flex; justify-content: flex-start; gap: 8px; flex-wrap: nowrap; }}
        .player-card {{ text-align: center; font-size: 13px; width: 100px; font-weight: bold; color: #4a5568; background: #f8fafc; padding: 12px 4px; border-radius: 8px; border: 1px solid #edf2f7; }}
        .player-role-label {{ font-size: 12px; color: #2b6cb0; font-weight: 950; margin-bottom: 8px; display: flex; align-items: center; justify-content: center; text-transform: uppercase; height: 32px; overflow: visible; line-height: 1.1; }}
        .player-card img {{ width: 52px; height: 52px; border-radius: 50%; border: 2px solid #cbd5e0; object-fit: cover; margin-bottom: 6px; background: #fff; }}
        .player-pos {{ font-size: 10px; color: #fff; background: #718096; padding: 3px 7px; border-radius: 4px; text-transform: uppercase; display: inline-block; margin-top: 6px; font-weight: bold; }}
        .metric-adv {{ font-size: 17px; font-weight: 700; color: #2d3748; }} .metric-big {{ font-size: 17px; font-weight: 800; color: #2d3748; }} .metric-huge {{ font-size: 19px; font-weight: 900; }}
        .text-green {{ color: #38a169; }} .text-red {{ color: #e53e3e; }} .text-blue {{ color: #2b6cb0; }}
        .footer {{ position: fixed; bottom: 0; left: 0; width: 100%; background: #2d3748; color: #cbd5e0; text-align: center; padding: 15px 0; font-size: 14px; font-weight: 500; border-top: 4px solid #ed8936; z-index: 100; box-shadow: 0 -2px 10px rgba(0,0,0,0.2); }}
        .footer a {{ color: #fff; text-decoration: none; font-weight: bold; }}
    </style></head><body>
        <div class="top-banner"><div class="top-logos"><img src="data:image/png;base64,{get_image_base64(LOGO_FEB)}" class="logo-side"><img src="data:image/png;base64,{get_image_base64(LOGO_EMPRESA)}" class="logo-center"><img src="data:image/png;base64,{get_image_base64(LOGO_LIGA)}" class="logo-side"></div>
        <div class="header-title-block"><h1>Contextual Lineups</h1><div class="subtitle">{context_str} | Min {m_filt} minutes played</div></div></div>
        <div class="legend-grid">
            <div class="legend-item"><b>TOTAL MIN:</b> Sample minutes.<br><b>PTS /40:</b> Proj. points per 40 mins.<br><b>PA /40:</b> Proj. allowed per 40 mins.</div>
            <div class="legend-item"><b>NET RTG /40:</b> Point diff. per 40 mins.<br><b>TS% *:</b> True Shooting %.<br><b>eFG% *:</b> Effective Field Goal %.</div>
            <div class="legend-item"><b>TOV% *:</b> Turnover Percentage.<br><b>ORB% *:</b> Offensive Rebound %.<br><b>FTr *:</b> Free Throw Rate.<br><b>USG% *:</b> Usage Percentage.</div>
            <div class="legend-item"><i>* Analytical Note:</i><br>Metrics represent the theoretical average of the 5 players based on individual season performance.</div>
        </div>
        <div class="team-section"><div class="team-title-block"><img src="{logo_url}" class="team-shield"><h2>{eq}</h2></div><div class="table-title title-top">Most Efficient Lineups</div>{render_table(top3)}"""
    
    if not bottom3.empty: html_content += f"""<div class="table-title title-bot">Least Efficient Lineups</div>{render_table(bottom3)}"""
    html_content += "</div>"
    html_content += """<div class="footer">© 2026 Analizing Basketball | <a href="https://www.analizingbasketball.com" target="_blank">www.analizingbasketball.com</a></div></body></html>"""
    return html_content

def HTML_BOXSCORE_AGREGADO(df_all_box, eq_objetivo, context_str, team_games_count):
    load_m14_mappings()
    eq_clean = clear_string(eq_objetivo); logo_url = "https://via.placeholder.com/60"
    for k, v in dicc_logos.items():
        if clear_string(k) == eq_clean or eq_clean in clear_string(k): logo_url = v; break
    
    html_tables = ""
    teams_data = {}
    df_all_box['GP'] = 1
    
    for team in [eq_objetivo, "OPPONENTS"]:
        t_df = df_all_box[df_all_box['Team'] == team].copy()
        agg_funcs = {'GP': 'sum', 'Starter': 'sum', 'Min_Sec_Num': 'sum', 'PTS': 'sum', 'PIR': 'sum', '2PM': 'sum', '2PA': 'sum', '3PM': 'sum', '3PA': 'sum', 'FTM': 'sum', 'FTA': 'sum', 'OREB': 'sum', 'DREB': 'sum', 'TREB': 'sum', 'AST': 'sum', 'STL': 'sum', 'TOV': 'sum', 'BLK': 'sum', 'FD': 'sum', 'PF': 'sum', '+/-': 'sum'}
        for c in agg_funcs.keys():
            t_df[c] = pd.to_numeric(t_df[c], errors='coerce').fillna(0)
        t_df_grouped = t_df.groupby(['Player_ID', 'Player', 'Logo_URL'], dropna=False).agg(agg_funcs).reset_index()
        t_df_grouped['sort_idx'] = t_df_grouped['Player_ID'].apply(get_classic_order)
        t_df_grouped = t_df_grouped.sort_values(by=['sort_idx', 'Min_Sec_Num'], ascending=[True, False])
        
        t_tot = {k: t_df_grouped[k].sum() for k in agg_funcs.keys()}
        p_list = [row.to_dict() for _, row in t_df_grouped.iterrows()]
        teams_data[team] = {'players': p_list, 'totals': t_tot}

    team = eq_objetivo; opp_team = "OPPONENTS"
    t_data, opp_data = teams_data[team], teams_data[opp_team]
    t_tot, opp_tot = t_data['totals'], opp_data['totals']
    
    tm_MIN_sec = t_tot['Min_Sec_Num'] if t_tot['Min_Sec_Num'] > 0 else 200 * 60 * 5 
    tm_FGA = t_tot['2PA'] + t_tot['3PA']; tm_FGM = t_tot['2PM'] + t_tot['3PM']
    opp_FGA = opp_tot['2PA'] + opp_tot['3PA']
    tm_Poss = tm_FGA + 0.44 * t_tot['FTA'] + t_tot['TOV']
    opp_Poss = opp_FGA + 0.44 * opp_tot['FTA'] + opp_tot['TOV']

    html_tables += f"""<h2 class="team-section-title">{team}</h2><div class="table-container"><table><thead class="group-headers"><tr><th colspan="5" class="bg-info">INFO</th><th colspan="13" class="bg-trad">TRADITIONAL (PER GAME)</th><th colspan="9" class="bg-shoot">SHOOTING (PER GAME)</th><th colspan="15" class="bg-adv">ADVANCED METRICS (AGGREGATED)</th></tr></thead><thead class="col-headers"><tr><th>PIC</th><th>PLAYER</th><th>ROLE</th><th>GP</th><th>GS</th><th>MIN</th><th>PTS</th><th>PIR</th><th>ORB</th><th>DRB</th><th>TRB</th><th>AST</th><th>STL</th><th>TOV</th><th>BLK</th><th>PFD</th><th>PF</th><th>+/-</th><th>2PM</th><th>2PA</th><th>2P%</th><th>3PM</th><th>3PA</th><th>3P%</th><th>FTM</th><th>FTA</th><th>FT%</th><th>GmSc</th><th>TS%</th><th>eFG%</th><th>3PAr</th><th>FTr</th><th>USG%</th><th>ORB%</th><th>DRB%</th><th>TRB%</th><th>AST%</th><th>STL%</th><th>BLK%</th><th>TOV%</th><th>PPP</th><th>PPS</th></tr></thead><tbody>"""
    
    tot_gmsc = 0
    for p in t_data['players']:
        pid = str(p.get('Player_ID', '')); pid = pid[:-2] if pid.endswith('.0') else pid
        
        p_data = custom_photos.get(pid, {})
        full_name = p_data.get("PLAYER_NAME", map_name.get(pid, p['Player']))
        player = get_short_name(full_name)
        role = map_role.get(pid, "N/A"); foto = p.get('Logo_URL'); foto = "https://via.placeholder.com/40/cbd5e0/ffffff?text=+" if pd.isna(foto) else foto
        
        gp = int(p['GP']) if int(p['GP']) > 0 else 1
        gs = int(p['Starter'])
            
        mins_pg = p['Min_Sec_Num'] / gp
        mins, secs = divmod(int(mins_pg), 60)
        
        if p['Min_Sec_Num'] <= 0:
            html_tables += f"<tr><td class='td-info'><img src='{foto}' class='player-photo'></td><td class='td-info player-name'>{player}</td><td class='td-info font-bold text-blue' style='font-size:11px;'>{role}</td><td class='td-info font-bold text-gray'>{gp}</td><td class='td-info font-bold text-blue'>{gs}</td><td class='td-info'><b>00:00</b></td><td colspan='36' class='td-trad text-center'>Did Not Play</td></tr>"
            continue
            
        pm_pg = p['+/-'] / gp; pm_str = f"+{pm_pg:.1f}" if pm_pg > 0 else f"{pm_pg:.1f}"
        pm_class = "text-green" if pm_pg > 0 else ("text-red" if pm_pg < 0 else "")
        pir_pg = p['PIR'] / gp; pir_class = "text-green" if pir_pg > 0 else ("text-red" if pir_pg < 0 else "")
        
        fgm = p['2PM'] + p['3PM']; fga = p['2PA'] + p['3PA']
        
        ts_denom = 2 * (fga + 0.44 * p['FTA'])
        ts_pct = (p['PTS'] / ts_denom * 100) if ts_denom > 0 else 0
        efg_pct = ((fgm + 0.5 * p['3PM']) / fga * 100) if fga > 0 else 0
        par3 = (p['3PA'] / fga * 100) if fga > 0 else 0; ftr = (p['FTA'] / fga * 100) if fga > 0 else 0
        
        usg_denom = p['Min_Sec_Num'] * tm_Poss
        usg_pct = 100 * ((fga + 0.44 * p['FTA'] + p['TOV']) * (tm_MIN_sec / 5)) / usg_denom if usg_denom > 0 else 0
        
        orb_denom = p['Min_Sec_Num'] * (t_tot['OREB'] + opp_tot['DREB'])
        orb_pct = 100 * (p['OREB'] * (tm_MIN_sec / 5)) / orb_denom if orb_denom > 0 else 0
        
        drb_denom = p['Min_Sec_Num'] * (t_tot['DREB'] + opp_tot['OREB'])
        drb_pct = 100 * (p['DREB'] * (tm_MIN_sec / 5)) / drb_denom if drb_denom > 0 else 0
        
        trb_denom = p['Min_Sec_Num'] * (t_tot['TREB'] + opp_tot['TREB'])
        trb_pct = 100 * (p['TREB'] * (tm_MIN_sec / 5)) / trb_denom if trb_denom > 0 else 0
        
        ast_denom = (((p['Min_Sec_Num'] / (tm_MIN_sec / 5)) * tm_FGM) - fgm) if tm_MIN_sec > 0 else 0
        ast_pct = 100 * p['AST'] / ast_denom if ast_denom > 0 else 0
        
        stl_denom = p['Min_Sec_Num'] * opp_Poss
        stl_pct = 100 * (p['STL'] * (tm_MIN_sec / 5)) / stl_denom if stl_denom > 0 else 0
        
        blk_denom = p['Min_Sec_Num'] * opp_tot['2PA']
        blk_pct = 100 * (p['BLK'] * (tm_MIN_sec / 5)) / blk_denom if blk_denom > 0 else 0
        
        tov_denom = fga + 0.44 * p['FTA'] + p['TOV']
        tov_pct = (100 * p['TOV'] / tov_denom) if tov_denom > 0 else 0
        
        gmsc = p['PTS'] + 0.4 * fgm - 0.7 * fga - 0.4 * (p['FTA'] - p['FTM']) + 0.7 * p['OREB'] + 0.3 * p['DREB'] + p['STL'] + 0.7 * p['AST'] + 0.7 * p['BLK'] - 0.4 * p['PF'] - p['TOV']
        tot_gmsc += gmsc
        ppp = (p['PTS'] / tov_denom) if tov_denom > 0 else 0; pps = (p['PTS'] / fga) if fga > 0 else 0
        fg2_pct = (p['2PM']/p['2PA']*100) if p['2PA'] > 0 else 0; fg3_pct = (p['3PM']/p['3PA']*100) if p['3PA'] > 0 else 0; ft_pct = (p['FTM']/p['FTA']*100) if p['FTA'] > 0 else 0
        
        html_tables += f"<tr><td class='td-info'><img src='{foto}' class='player-photo'></td><td class='td-info player-name'>{player}</td><td class='td-info font-bold text-blue' style='font-size:11px;'>{role}</td><td class='td-info font-bold text-gray'>{gp}</td><td class='td-info font-bold text-blue'>{gs}</td><td class='td-info'><b>{mins:02d}:{secs:02d}</b></td><td class='td-trad font-bold text-blue'>{p['PTS']/gp:.1f}</td><td class='td-trad font-bold {pir_class}'>{pir_pg:.1f}</td><td class='td-trad'>{p['OREB']/gp:.1f}</td><td class='td-trad'>{p['DREB']/gp:.1f}</td><td class='td-trad font-bold'>{p['TREB']/gp:.1f}</td><td class='td-trad'>{p['AST']/gp:.1f}</td><td class='td-trad text-green'>{p['STL']/gp:.1f}</td><td class='td-trad text-red'>{p['TOV']/gp:.1f}</td><td class='td-trad'>{p['BLK']/gp:.1f}</td><td class='td-trad text-gray'>{p['FD']/gp:.1f}</td><td class='td-trad text-gray'>{p['PF']/gp:.1f}</td><td class='td-trad font-bold {pm_class}'>{pm_str}</td><td class='td-shoot font-bold'>{p['2PM']/gp:.1f}</td><td class='td-shoot text-gray'>{p['2PA']/gp:.1f}</td><td class='td-shoot'>{fg2_pct:.0f}%</td><td class='td-shoot font-bold'>{p['3PM']/gp:.1f}</td><td class='td-shoot text-gray'>{p['3PA']/gp:.1f}</td><td class='td-shoot'>{fg3_pct:.0f}%</td><td class='td-shoot font-bold'>{p['FTM']/gp:.1f}</td><td class='td-shoot text-gray'>{p['FTA']/gp:.1f}</td><td class='td-shoot'>{ft_pct:.0f}%</td><td class='td-adv font-bold'>{gmsc/gp:.1f}</td><td class='td-adv'>{ts_pct:.1f}%</td><td class='td-adv'>{efg_pct:.1f}%</td><td class='td-adv text-gray'>{par3:.1f}%</td><td class='td-adv text-gray'>{ftr:.1f}%</td><td class='td-adv font-bold text-blue'>{usg_pct:.1f}%</td><td class='td-adv'>{orb_pct:.1f}%</td><td class='td-adv'>{drb_pct:.1f}%</td><td class='td-adv text-gray'>{trb_pct:.1f}%</td><td class='td-adv'>{ast_pct:.1f}%</td><td class='td-adv'>{stl_pct:.1f}%</td><td class='td-adv'>{blk_pct:.1f}%</td><td class='td-adv'>{tov_pct:.1f}%</td><td class='td-adv font-bold'>{ppp:.2f}</td><td class='td-adv font-bold'>{pps:.2f}</td></tr>"
        
    team_gp = team_games_count if team_games_count > 0 else 1
    tm_mins_pg = (t_tot['Min_Sec_Num'] / team_gp) / 5
    tm_mins, tm_secs = divmod(int(tm_mins_pg), 60); tm_ts_denom = 2 * (tm_FGA + 0.44 * t_tot['FTA'])
    
    html_tables += f"<tr class='total-row'><td colspan='5' class='td-info' style='text-align: right; padding-right: 15px;'><b>TEAM AVERAGES</b></td><td class='td-info'><b>{tm_mins:02d}:{tm_secs:02d}</b></td><td class='td-trad font-bold text-blue'>{t_tot['PTS']/team_gp:.1f}</td><td class='td-trad font-bold'>{t_tot['PIR']/team_gp:.1f}</td><td class='td-trad'>{t_tot['OREB']/team_gp:.1f}</td><td class='td-trad'>{t_tot['DREB']/team_gp:.1f}</td><td class='td-trad font-bold'>{t_tot['TREB']/team_gp:.1f}</td><td class='td-trad'>{t_tot['AST']/team_gp:.1f}</td><td class='td-trad text-green'>{t_tot['STL']/team_gp:.1f}</td><td class='td-trad text-red'>{t_tot['TOV']/team_gp:.1f}</td><td class='td-trad'>{t_tot['BLK']/team_gp:.1f}</td><td class='td-trad text-gray'>{t_tot['FD']/team_gp:.1f}</td><td class='td-trad text-gray'>{t_tot['PF']/team_gp:.1f}</td><td class='td-trad'></td><td class='td-shoot font-bold'>{t_tot['2PM']/team_gp:.1f}</td><td class='td-shoot text-gray'>{t_tot['2PA']/team_gp:.1f}</td><td class='td-shoot'>{(t_tot['2PM']/t_tot['2PA']*100) if t_tot['2PA']>0 else 0:.0f}%</td><td class='td-shoot font-bold'>{t_tot['3PM']/team_gp:.1f}</td><td class='td-shoot text-gray'>{t_tot['3PA']/team_gp:.1f}</td><td class='td-shoot'>{(t_tot['3PM']/t_tot['3PA']*100) if t_tot['3PA']>0 else 0:.0f}%</td><td class='td-shoot font-bold'>{t_tot['FTM']/team_gp:.1f}</td><td class='td-shoot text-gray'>{t_tot['FTA']/team_gp:.1f}</td><td class='td-shoot'>{(t_tot['FTM']/t_tot['FTA']*100) if t_tot['FTA']>0 else 0:.0f}%</td><td class='td-adv font-bold'>{tot_gmsc/team_gp:.1f}</td><td class='td-adv'>{(t_tot['PTS']/tm_ts_denom*100) if tm_ts_denom>0 else 0:.1f}%</td><td class='td-adv'>{((tm_FGM + 0.5 * t_tot['3PM'])/tm_FGA*100) if tm_FGA>0 else 0:.1f}%</td><td class='td-adv text-gray'>{(t_tot['3PA']/tm_FGA*100) if tm_FGA>0 else 0:.1f}%</td><td class='td-adv text-gray'>{(t_tot['FTA']/tm_FGA*100) if tm_FGA>0 else 0:.1f}%</td><td class='td-adv font-bold text-blue'>100.0%</td><td class='td-adv'>{(100*t_tot['OREB']/(t_tot['OREB']+opp_tot['DREB'])) if (t_tot['OREB']+opp_tot['DREB'])>0 else 0:.1f}%</td><td class='td-adv'>{(100*t_tot['DREB']/(t_tot['DREB']+opp_tot['OREB'])) if (t_tot['DREB']+opp_tot['OREB'])>0 else 0:.1f}%</td><td class='td-adv text-gray'>{(100*t_tot['TREB']/(t_tot['TREB']+opp_tot['TREB'])) if (t_tot['TREB']+opp_tot['TREB'])>0 else 0:.1f}%</td><td class='td-adv'>{(100*t_tot['AST']/tm_FGM) if tm_FGM>0 else 0:.1f}%</td><td class='td-adv'>{(100*t_tot['STL']/opp_Poss) if opp_Poss>0 else 0:.1f}%</td><td class='td-adv'>{(100*t_tot['BLK']/opp_tot['2PA']) if opp_tot['2PA']>0 else 0:.1f}%</td><td class='td-adv'>{(100*t_tot['TOV']/(tm_FGA+0.44*t_tot['FTA']+t_tot['TOV'])) if (tm_FGA+0.44*t_tot['FTA']+t_tot['TOV'])>0 else 0:.1f}%</td><td class='td-adv font-bold'>{(t_tot['PTS']/(tm_FGA+0.44*t_tot['FTA']+t_tot['TOV'])) if (tm_FGA+0.44*t_tot['FTA']+t_tot['TOV'])>0 else 0:.2f}</td><td class='td-adv font-bold'>{(t_tot['PTS']/tm_FGA) if tm_FGA>0 else 0:.2f}</td></tr></tbody></table></div>"

    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Aggregated Boxscore: {eq_objetivo}</title>
    <style>
        body {{ font-family: 'Segoe UI', sans-serif; background: #f4f6f9; color: #1a202c; margin: 0; padding: 20px; padding-bottom: 80px; }}
        .header-container {{ background: #fff; padding: 20px 30px; border-radius: 12px; margin-bottom: 25px; }}
        .top-logos {{ display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #edf2f7; padding-bottom: 15px; margin-bottom: 20px; }}
        .logo-side {{ height: 60px; max-width: 130px; object-fit: contain; }} .logo-center {{ height: 90px; max-width: 250px; object-fit: contain; }}
        .team-score-block {{ display: flex; justify-content: center; align-items: center; gap: 30px; }} .team-shield {{ width: 90px; height: 90px; object-fit: contain; }}
        .team-score-block h1 {{ margin: 0; font-size: 36px; color: #2d3748; }} 
        .scores {{ font-size: 22px; color: #2d3748; margin-top: 15px; font-weight: 900; text-align: center; letter-spacing: 1px; line-height: 1.4; }}
        .team-section-title {{ color: #fff; background: #2d3748; padding: 10px 20px; border-radius: 8px; margin-top: 40px; margin-bottom: 15px; font-size: 22px; }}
        .table-container {{ background: #fff; border-radius: 12px; overflow-x: auto; margin-bottom: 25px; }}
        table {{ width: 100%; border-collapse: collapse; text-align: center; white-space: nowrap; }}
        .bg-info {{ background: #2d3748 !important; color: #fff !important; }} .bg-trad {{ background: #4a5568 !important; color: #fff !important; }} .bg-shoot {{ background: #2b6cb0 !important; color: #fff !important; }} .bg-adv {{ background: #2c7a7b !important; color: #fff !important; }}
        .col-headers th {{ background: #edf2f7; color: #4a5568; font-size: 11px; padding: 10px 5px; border-bottom: 2px solid #cbd5e0; }}
        td {{ padding: 10px 6px; font-size: 14px; border-bottom: 1px solid #edf2f7; vertical-align: middle; }}
        .td-info {{ background: #ffffff; }} .td-trad {{ background: #f8fafc; }} .td-shoot {{ background: #ebf8fa; }} .td-adv {{ background: #f0fff4; }}
        .total-row td {{ background: #e2e8f0; font-weight: bold; border-top: 2px solid #a0aec0; }}
        .player-name {{ text-align: left; font-weight: 700; color: #2d3748; font-size: 14px; }} .player-photo {{ width: 36px; height: 36px; border-radius: 50%; border: 2px solid #cbd5e0; object-fit: cover; }}
        .text-blue {{ color: #2b6cb0; }} .text-red {{ color: #e53e3e; }} .text-green {{ color: #38a169; }} .text-gray {{ color: #a0aec0; }} .font-bold {{ font-weight: 800; }}
        .legend-grid {{ display: grid; grid-template-columns: repeat(5, 1fr); gap: 15px; background: #fff; padding: 25px; border-radius: 12px; margin-top: 30px; margin-bottom: 40px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); }}
        .legend-item {{ font-size: 11px; color: #4a5568; line-height: 1.6; text-align: left; }} .legend-item b {{ color: #2d3748; }}
        .footer {{ position: fixed; bottom: 0; left: 0; width: 100%; background: #2d3748; color: #cbd5e0; text-align: center; padding: 15px 0; font-size: 14px; font-weight: 500; border-top: 4px solid #ed8936; z-index: 100; box-shadow: 0 -2px 10px rgba(0,0,0,0.2); }}
        .footer a {{ color: #fff; text-decoration: none; font-weight: bold; }}
    </style></head><body>
        <div class="header-container"><div class="top-logos"><img src="data:image/png;base64,{get_image_base64(LOGO_FEB)}" class="logo-side"><img src="data:image/png;base64,{get_image_base64(LOGO_EMPRESA)}" class="logo-center"><img src="data:image/png;base64,{get_image_base64(LOGO_LIGA)}" class="logo-side"></div>
        <div class="match-info">
            <div class="team-score-block"><img src="{logo_url}" class="team-shield"><h1>{eq_objetivo}</h1></div>
            <div class="scores">AGGREGATED BOXSCORE (PER GAME)<br><span style="font-weight: 600; font-size: 17px; color: #718096;">{context_str}</span></div>
        </div></div>
        {html_tables}
        <div class="legend-grid"><div class="legend-item"><b>PIC / PLAYER:</b> Player Info.<br><b>GP:</b> Games Played.<br><b>GS:</b> Games Started.<br><b>MIN:</b> Minutes Played.<br><b>PTS:</b> Points Scored.<br><b>PIR:</b> Performance Index Rating.<br><b>+/-:</b> Plus/Minus point differential.</div><div class="legend-item"><b>ORB:</b> Offensive Rebounds.<br><b>DRB:</b> Defensive Rebounds.<br><b>TRB:</b> Total Rebounds.<br><b>AST:</b> Assists.<br><b>STL:</b> Steals.<br><b>TOV:</b> Turnovers.</div><div class="legend-item"><b>BLK:</b> Blocks.<br><b>PFD:</b> Personal Fouls Drawn.<br><b>PF:</b> Personal Fouls Committed.<br><b>2PM/A:</b> 2-Point Goals Made/Attempted.<br><b>3PM/A:</b> 3-Point Goals Made/Attempted.<br><b>FTM/A:</b> Free Throws Made/Attempted.</div><div class="legend-item"><b>GmSc:</b> Game Score (Productivity metric).<br><b>TS%:</b> True Shooting Percentage.<br><b>eFG%:</b> Effective Field Goal Percentage.<br><b>3PAr:</b> 3-Point Attempt Rate.<br><b>FTr:</b> Free Throw Attempt Rate.<br><b>USG%:</b> Usage Percentage.</div><div class="legend-item"><b>ORB% / DRB% / TRB%:</b> Rebound Percentages.<br><b>AST%:</b> Assist Percentage.<br><b>STL% / BLK%:</b> Steal & Block Percentages.<br><b>TOV%:</b> Turnover Percentage.<br><b>PPP:</b> Points Per Possession.<br><b>PPS:</b> Points Per Shot.</div></div>
        <div class="footer">© 2026 Analizing Basketball | <a href="https://www.analizingbasketball.com" target="_blank">www.analizingbasketball.com</a></div>
    </body></html>
    """
    return html

# ==============================================================================
# 7. INTERFAZ API REST
# ==============================================================================
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/generar", response_class=HTMLResponse)
def generar_scouting(jornada: int = 22, equipo: str = "MOVISTAR ESTUDIANTES", tipo_reporte: str = "quintetos"):
    if not os.path.exists(os.path.join(DATA_DIR, "logos_equipos.json")): extraer_diccionario_logos()
    if not os.path.exists(os.path.join(DATA_DIR, "calendario_maestro_primerafeb_2025.csv")): construir_calendario_maestro()
    if not os.path.exists(os.path.join(DATA_DIR, "maestro_jugadores_primerafeb.csv")): extraer_maestro_jugadores()
    
    partidos = obtener_partidos_jornada(jornada)
    if equipo != 'TODOS':
        equipo_seleccionado = equipo.upper()
        partidos = [p for p in partidos if equipo_seleccionado == p['equipo_local'].upper() or equipo_seleccionado == p['equipo_visitante'].upper()]
        
    if not partidos: raise HTTPException(status_code=404, detail="No se encontraron partidos para esa combinación de jornada y equipo.")
        
    p = partidos[0]
    if not p['jugado']: raise HTTPException(status_code=400, detail="El partido aún no se ha disputado.")
        
    if not extraer_partido_api(p['match_id']): raise HTTPException(status_code=500, detail="Error al descargar datos en vivo.")
        
    ruta_pbp_clean, ruta_box_clean = limpiar_y_avanzadas(p['match_id'], p['equipo_local'], p['equipo_visitante'], jornada)
    
    if tipo_reporte.lower() == "quintetos":
        ruta_final = generar_html_quintetos(ruta_pbp_clean, ruta_box_clean, p['match_id'], p['equipo_local'], p['equipo_visitante'], p['fecha'])
    else:
        ruta_final = generar_html_boxscore(ruta_box_clean, ruta_pbp_clean, p['match_id'], p['equipo_local'], p['equipo_visitante'], p['fecha'])
    
    with open(ruta_final, "r", encoding="utf-8") as f: html_content = f.read()
    return HTMLResponse(content=html_content, status_code=200)

@app.get("/splits", response_class=HTMLResponse)
def generar_splits(s_rnd: int = 1, e_rnd: int = 22, eq: str = "MOVISTAR ESTUDIANTES", m_filt: int = 10):
    if s_rnd > e_rnd: raise HTTPException(status_code=400, detail="La jornada de inicio no puede ser posterior a la final.")
    
    if not os.path.exists(FILE_LINEUPS): raise HTTPException(status_code=404, detail="Archivo LINEUPS maestro no encontrado.")
        
    load_m14_mappings()

    df_lineups_master = pd.read_csv(FILE_LINEUPS)
    df_lineups_master['TEAM'] = df_lineups_master['TEAM'].replace({'CLUB OURENSE BALONCESTO': 'CLOUD.GAL OURENSE BALONCESTO', 'OURENSE BALONCESTO': 'CLOUD.GAL OURENSE BALONCESTO'})
    for col in ['P1_ID', 'P2_ID', 'P3_ID', 'P4_ID', 'P5_ID']: df_lineups_master[col] = df_lineups_master[col].apply(safe_id)

    if 'ROUND' in df_lineups_master.columns:
        df_lineups_master['ROUND_NUM'] = pd.to_numeric(df_lineups_master['ROUND'], errors='coerce').fillna(0).astype(int)
        df_split = df_lineups_master[(df_lineups_master['ROUND_NUM'] >= s_rnd) & (df_lineups_master['ROUND_NUM'] <= e_rnd)].copy()
    else: df_split = df_lineups_master.copy()
        
    if eq != "TODOS": df_split = df_split[df_split['TEAM'] == eq].copy()
    if df_split.empty: raise HTTPException(status_code=404, detail="No hay datos para este tramo.")
        
    df_split[['ARCHETYPE', 'REAL_LINEUP']] = df_split.apply(create_signatures, axis=1)
    df_valid = df_split[df_split['ARCHETYPE'] != "Incomplete"].copy()

    arch_stats = df_valid.groupby(['TEAM', 'ARCHETYPE']).agg({'MINUTES': 'sum', 'PTS_FOR': 'sum', 'PTS_AGAINST': 'sum'}).reset_index()
    arch_stats = arch_stats[arch_stats['MINUTES'] >= m_filt].copy()
    if arch_stats.empty: raise HTTPException(status_code=404, detail="Ningún quinteto superó el filtro de minutos.")

    real_lineup_stats = df_valid.groupby(['TEAM', 'ARCHETYPE', 'REAL_LINEUP']).agg({'MINUTES': 'sum'}).reset_index()
    best_real_lineups = real_lineup_stats.sort_values('MINUTES', ascending=False).drop_duplicates(subset=['TEAM', 'ARCHETYPE'])

    efficiency = pd.merge(arch_stats, best_real_lineups[['TEAM', 'ARCHETYPE', 'REAL_LINEUP']], on=['TEAM', 'ARCHETYPE'])
    efficiency['PTS_40'] = np.where(efficiency['MINUTES'] > 0, ((efficiency['PTS_FOR'] / efficiency['MINUTES']) * 40), 0)
    efficiency['PA_40'] = np.where(efficiency['MINUTES'] > 0, ((efficiency['PTS_AGAINST'] / efficiency['MINUTES']) * 40), 0)
    efficiency['NET_RATING'] = (efficiency['PTS_40'] - efficiency['PA_40']).round(1)
    for col in ['PTS_40', 'PA_40']: efficiency[col] = efficiency[col].round(1)

    logo_empresa_b64, logo_feb_b64, logo_liga_b64 = get_image_base64(LOGO_EMPRESA), get_image_base64(LOGO_FEB), get_image_base64(LOGO_LIGA)
    eq_name_display = eq if eq != "TODOS" else "League Wide"
    round_title = f"Rounds {s_rnd} to {e_rnd}"
    
    html_content = f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Tactical Splits: {eq_name_display}</title>
    <style>
        body {{ font-family: 'Segoe UI', sans-serif; background: #f4f6f9; color: #333; margin: 0; padding: 20px; padding-bottom: 80px; }}
        .top-banner {{ background: #fff; padding: 20px 30px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 20px; }}
        .top-logos {{ display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #e2e8f0; padding-bottom: 15px; margin-bottom: 15px; }}
        .logo-side {{ height: 60px; max-width: 130px; object-fit: contain; }} .logo-center {{ height: 90px; max-width: 250px; object-fit: contain; }}
        .header-title-block {{ text-align: center; }} h1 {{ margin: 0; font-size: 32px; color: #1a202c; text-transform: uppercase; }}
        .subtitle {{ color: #e53e3e; font-size: 16px; margin-top: 10px; font-weight: bold; }}
        .legend-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 20px; background: #fff; padding: 20px; border-radius: 8px; border: 1px solid #e2e8f0; margin-bottom: 40px; }}
        .legend-item {{ font-size: 12px; color: #4a5568; line-height: 1.6; text-align: left; }} .legend-item b {{ color: #2d3748; }}
        .team-section {{ background: #fff; border-radius: 12px; padding: 20px; margin-bottom: 40px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); border: 1px solid #e2e8f0; }}
        .team-title-block {{ display: flex; align-items: center; gap: 15px; border-bottom: 3px solid #2b6cb0; padding-bottom: 10px; margin-bottom: 20px; }}
        .team-shield {{ width: 60px; height: 60px; object-fit: contain; }} h2 {{ margin: 0; font-size: 28px; color: #2d3748; text-transform: uppercase; font-weight: 800; }}
        .table-title {{ font-size: 18px; font-weight: bold; margin-bottom: 10px; padding-left: 10px; border-left: 5px solid; text-transform: uppercase; }}
        .title-top {{ border-color: #2b6cb0; color: #2b6cb0; }} .title-bot {{ border-color: #718096; color: #4a5568; }}
        .table-container {{ overflow-x: auto; margin-bottom: 30px; }} table {{ width: 100%; min-width: 1100px; border-collapse: collapse; text-align: center; table-layout: fixed; }}
        th {{ background: #2d3748; color: #fff; padding: 18px 6px; font-size: 16px; font-weight: 800; text-transform: uppercase; }} 
        td {{ padding: 14px 4px; border-bottom: 1px solid #e2e8f0; vertical-align: middle; }}
        th.col-lineup {{ width: 40%; text-align: left; padding-left: 15px; }} .players-flex {{ display: flex; justify-content: flex-start; gap: 8px; flex-wrap: nowrap; }}
        .player-card {{ text-align: center; font-size: 13px; width: 100px; font-weight: bold; color: #4a5568; background: #f8fafc; padding: 12px 4px; border-radius: 8px; border: 1px solid #edf2f7; }}
        .player-role-label {{ font-size: 12px; color: #2b6cb0; font-weight: 950; margin-bottom: 8px; display: flex; align-items: center; justify-content: center; text-transform: uppercase; height: 32px; overflow: visible; line-height: 1.1; }}
        .player-card img {{ width: 52px; height: 52px; border-radius: 50%; border: 2px solid #cbd5e0; object-fit: cover; margin-bottom: 6px; background: #fff; }}
        .player-pos {{ font-size: 10px; color: #fff; background: #718096; padding: 3px 7px; border-radius: 4px; text-transform: uppercase; display: inline-block; margin-top: 6px; font-weight: bold; }}
        .metric-adv {{ font-size: 17px; font-weight: 700; color: #2d3748; }} .metric-big {{ font-size: 17px; font-weight: 800; color: #2d3748; }} .metric-huge {{ font-size: 19px; font-weight: 900; }}
        .text-green {{ color: #38a169; }} .text-red {{ color: #e53e3e; }} .text-blue {{ color: #2b6cb0; }}
        .footer {{ position: fixed; bottom: 0; left: 0; width: 100%; background: #2d3748; color: #cbd5e0; text-align: center; padding: 15px 0; font-size: 13px; border-top: 3px solid #ed8936; }}
    </style></head><body>
        <div class="top-banner"><div class="top-logos"><img src="data:image/png;base64,{logo_feb_b64}" class="logo-side"><img src="data:image/png;base64,{logo_empresa_b64}" class="logo-center"><img src="data:image/png;base64,{logo_liga_b64}" class="logo-side"></div>
        <div class="header-title-block"><h1>Tactical Splits Slicer</h1><div class="subtitle">Primera FEB | {round_title} | Filter: Min {m_filt} minutes played</div></div></div>
        <div class="legend-grid">
            <div class="legend-item"><b>TOTAL MIN:</b> Split minutes.<br><b>PTS /40:</b> Proj. points per 40 mins.<br><b>PA /40:</b> Proj. allowed per 40 mins.</div>
            <div class="legend-item"><b>NET RTG /40:</b> Point diff. per 40 mins.<br><b>TS% *:</b> True Shooting %.<br><b>eFG% *:</b> Effective Field Goal %.</div>
            <div class="legend-item"><b>TOV% *:</b> Turnover Percentage.<br><b>ORB% *:</b> Offensive Rebound %.<br><b>FTr *:</b> Free Throw Rate.<br><b>USG% *:</b> Usage Percentage.</div>
            <div class="legend-item"><i>* Analytical Note:</i><br>Metrics represent the theoretical average of the 5 players based on individual season performance.</div>
        </div>
    """

    def render_table(df_subset):
        t_html = f"""<div class='table-container'><table><thead><tr>
            <th class='col-lineup'></th><th>TOTAL MIN</th><th>PTS /40</th><th>PA /40</th><th>NET RTG /40</th>
            <th style='background:#2c7a7b'>TS% *</th><th style='background:#2c7a7b'>eFG% *</th><th style='background:#2c7a7b'>TOV% *</th><th style='background:#2c7a7b'>ORB% *</th><th style='background:#2c7a7b'>FTr *</th><th style='background:#2c7a7b'>USG% *</th>
            </tr></thead><tbody>"""
        for _, row in df_subset.iterrows():
            pm_val = row['NET_RATING']
            color_class = "text-green" if pm_val > 0 else ("text-red" if pm_val < 0 else "")
            sign = "+" if pm_val > 0 else ""
            p_ids = row['REAL_LINEUP'].split("-")
            cards_html = ""
            avg_efg, avg_ts, avg_tov, avg_orb, avg_ftr, avg_usg, count = 0, 0, 0, 0, 0, 0, 0
            for pid in p_ids:
                name_short = get_short_name(map_name.get(pid, "Unknown"))
                pos = map_pos.get(pid, "N/A")
                role = map_role.get(pid, "Unknown Role")
                foto_url = f"https://imagenes.feb.es/Foto.aspx?c={pid}"
                cards_html += f"""<div class='player-card'><span class='player-role-label'>{role}</span><img src='{foto_url}' onerror="this.src='https://via.placeholder.com/50/cbd5e0/ffffff?text=+'"><br>{name_short}<br><span class='player-pos'>{pos}</span></div>"""
                if pid in map_efg:
                    avg_efg += map_efg.get(pid, 0); avg_ts += map_ts.get(pid, 0); avg_tov += map_tov.get(pid, 0)
                    avg_orb += map_orb.get(pid, 0); avg_ftr += map_ftr.get(pid, 0); avg_usg += map_usg.get(pid, 0); count += 1
            f_efg = f"{(avg_efg/count):.1f}%" if count > 0 else "N/A"
            f_ts = f"{(avg_ts/count):.1f}%" if count > 0 else "N/A"
            f_tov = f"{(avg_tov/count):.1f}%" if count > 0 else "N/A"
            f_orb = f"{(avg_orb/count):.1f}%" if count > 0 else "N/A"
            f_ftr = f"{(avg_ftr/count):.3f}" if count > 0 else "N/A"
            f_usg = f"{(avg_usg/count):.1f}%" if count > 0 else "N/A"
            t_html += f"""<tr><td style='text-align:left; padding-left:15px;'><div class='players-flex'>{cards_html}</div></td><td class='metric-big'>{row['MINUTES']:.1f}</td><td class='metric-big text-blue'>{row['PTS_40']:.1f}</td><td class='metric-big text-red'>{row['PA_40']:.1f}</td><td class='metric-huge {color_class}'>{sign}{pm_val}</td><td class='metric-adv' style='color:#B22222'>{f_ts}</td><td class='metric-adv'>{f_efg}</td><td class='metric-adv'>{f_tov}</td><td class='metric-adv'>{f_orb}</td><td class='metric-adv'>{f_ftr}</td><td class='metric-adv'>{f_usg}</td></tr>"""
        t_html += "</tbody></table></div>"
        return t_html

    equipos = sorted(efficiency['TEAM'].unique())
    for equipo in equipos:
        df_equipo = efficiency[efficiency['TEAM'] == equipo].sort_values(by='NET_RATING', ascending=False)
        top3 = df_equipo.head(3)
        bottom3 = df_equipo.loc[~df_equipo.index.isin(top3.index)].tail(3) if len(df_equipo) > 3 else pd.DataFrame()
        eq_clean = clear_string(equipo)
        logo_url = "https://via.placeholder.com/60"
        for k, v in dicc_logos.items():
            if clear_string(k) == eq_clean or eq_clean in clear_string(k): logo_url = v; break
        html_content += f"""<div class="team-section"><div class="team-title-block"><img src="{logo_url}" class="team-shield"><h2>{equipo}</h2></div><div class="table-title title-top">Most Efficient Lineups ({round_title})</div>{render_table(top3)}"""
        if not bottom3.empty: html_content += f"""<div class="table-title title-bot">Least Efficient Lineups ({round_title})</div>{render_table(bottom3)}"""
        html_content += "</div>"

    html_content += "<div class='footer'>© 2026 <b>Analizing Basketball</b> | MBDD TFM - Tactical Splits Slicer</div></body></html>"
    ruta_final = os.path.join(REPORTS_DIR, f"SPLIT_J{s_rnd}_J{e_rnd}.html")
    with open(ruta_final, "w", encoding="utf-8") as f: f.write(html_content)
    return ruta_final

@app.get("/contextual", response_class=HTMLResponse)
def generar_contextual(eq: str = "MOVISTAR ESTUDIANTES", venue: str = "ALL", n_games: int = 3, m_filt: int = 10, tipo_reporte: str = "quintetos"):
    if not os.path.exists(FILE_LINEUPS):
        raise HTTPException(status_code=404, detail="Archivo LINEUPS maestro no encontrado.")
        
    df_lineups = pd.read_csv(FILE_LINEUPS)
    df_lineups['TEAM'] = df_lineups['TEAM'].replace({'CLUB OURENSE BALONCESTO': 'CLOUD.GAL OURENSE BALONCESTO', 'OURENSE BALONCESTO': 'CLOUD.GAL OURENSE BALONCESTO'})
    
    df_team_games = df_lineups[df_lineups['TEAM'] == eq].copy()
    if venue != "ALL":
        df_team_games = df_team_games[df_team_games['LOCATION'] == venue]
        
    if df_team_games.empty:
        raise HTTPException(status_code=404, detail=f"No hay partidos jugados de {eq} en condición {venue}.")
        
    unique_games = df_team_games[['MATCHID', 'ROUND']].drop_duplicates()
    unique_games['ROUND_NUM'] = pd.to_numeric(unique_games['ROUND'], errors='coerce').fillna(0).astype(int)
    unique_games = unique_games.sort_values(by='ROUND_NUM', ascending=False).head(n_games)
    
    jornadas_validas = unique_games['ROUND_NUM'].tolist()
    match_ids = unique_games['MATCHID'].astype(str).tolist()
    context_str = f"Últimos {len(jornadas_validas)} Partidos Jugados | Venue: {venue}" if n_games != 99 else f"Season Wide | Venue: {venue}"
    
    load_m14_mappings()
    
    if tipo_reporte.lower() == "quintetos":
        df_split = df_lineups[(df_lineups['TEAM'] == eq) & (pd.to_numeric(df_lineups['ROUND'], errors='coerce').isin(jornadas_validas))].copy()
        if df_split.empty: raise HTTPException(status_code=404, detail="No hay datos de quintetos registrados para esas jornadas.")
        
        for col in ['P1_ID', 'P2_ID', 'P3_ID', 'P4_ID', 'P5_ID']: df_split[col] = df_split[col].apply(safe_id)
        df_split[['ARCHETYPE', 'REAL_LINEUP']] = df_split.apply(create_signatures, axis=1)
        df_valid = df_split[df_split['ARCHETYPE'] != "Incomplete"].copy()
        
        arch_stats = df_valid.groupby(['TEAM', 'ARCHETYPE']).agg({'MINUTES': 'sum', 'PTS_FOR': 'sum', 'PTS_AGAINST': 'sum'}).reset_index()
        arch_stats = arch_stats[arch_stats['MINUTES'] >= m_filt].copy()
        if arch_stats.empty: raise HTTPException(status_code=404, detail=f"Ningún quinteto superó el filtro de {m_filt} minutos.")
            
        real_lineup_stats = df_valid.groupby(['TEAM', 'ARCHETYPE', 'REAL_LINEUP']).agg({'MINUTES': 'sum'}).reset_index()
        best_real_lineups = real_lineup_stats.sort_values('MINUTES', ascending=False).drop_duplicates(subset=['TEAM', 'ARCHETYPE'])
        
        efficiency = pd.merge(arch_stats, best_real_lineups[['TEAM', 'ARCHETYPE', 'REAL_LINEUP']], on=['TEAM', 'ARCHETYPE'])
        efficiency['PTS_40'] = np.where(efficiency['MINUTES'] > 0, ((efficiency['PTS_FOR'] / efficiency['MINUTES']) * 40), 0)
        efficiency['PA_40'] = np.where(efficiency['MINUTES'] > 0, ((efficiency['PTS_AGAINST'] / efficiency['MINUTES']) * 40), 0)
        efficiency['NET_RATING'] = (efficiency['PTS_40'] - efficiency['PA_40']).round(1)
        for col in ['PTS_40', 'PA_40']: efficiency[col] = efficiency[col].round(1)

        html_content = HTML_LINEUPS_AGREGADOS(efficiency, eq, context_str, m_filt)
        return HTMLResponse(content=html_content, status_code=200)

    else:
        list_box_df = []
        eq_clean = clear_string(eq).replace("CLOUD.GAL ", "").replace("GRUPO CAESA SEGUROS ", "")
        for mid in match_ids:
            box_path = os.path.join(DATA_DIR, f"boxscore_{mid}.csv")
            if not os.path.exists(box_path): extraer_partido_api(mid)
            if os.path.exists(box_path):
                df_b_clean = limpiar_boxscore_api(mid)
                is_target = df_b_clean['Team'].apply(lambda x: eq_clean in clear_string(str(x)))
                df_b_clean.loc[~is_target, 'Team'] = "OPPONENTS"
                df_b_clean.loc[is_target, 'Team'] = eq
                list_box_df.append(df_b_clean)
                
        if not list_box_df: raise HTTPException(status_code=404, detail="No se pudieron procesar los boxscores de esos partidos.")
            
        df_all_box = pd.concat(list_box_df, ignore_index=True)
        html_content = HTML_BOXSCORE_AGREGADO(df_all_box, eq, context_str, len(jornadas_validas))
        return HTMLResponse(content=html_content, status_code=200)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
