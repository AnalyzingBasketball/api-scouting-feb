import os
import json
import re
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import traceback

# --- RUTAS BASE ---
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
DATA_DIR    = os.path.join(BASE_DIR, "data")
RAW_API_DIR = os.path.join(DATA_DIR, "raw_data")

os.makedirs(RAW_API_DIR, exist_ok=True)

# Fichero compartido de fotos/posiciones (independiente de la competición)
ARCHIVO_PHOTOS = os.path.join(DATA_DIR, "raw_data", "PLAYER_NAMES_DICT.json")

# ==============================================================================
# COMPETICIONES SOPORTADAS
# ==============================================================================
COMPETITIONS = {
    'primerafeb':  {'name': 'Primera FEB',  'url': 'https://www.feb.es/competiciones/calendario/primerafeb/1/2025',     'slug': 'PRIMERAFEB'},
    'lfendesa':    {'name': 'LF Endesa',    'url': 'https://www.feb.es/competiciones/calendario/lfendesa/4/2025',       'slug': 'LFENDESA'},
    'lfchallenge': {'name': 'LF Challenge', 'url': 'https://www.feb.es/competiciones/calendario/lfchallenge/67/2025',   'slug': 'LFCHALLENGE'},
    'segundafeb':  {'name': 'Segunda FEB',  'url': 'https://www.feb.es/competiciones/calendario/segundafeb/2/2025',     'slug': 'SEGUNDAFEB'},
    'lf2':         {'name': 'LF-2',         'url': 'https://www.feb.es/competiciones/calendario/lf2/9/2025',            'slug': 'LF2'},
    'tercerafeb':  {'name': 'Tercera FEB',  'url': 'https://www.feb.es/competiciones/calendario/tercerafeb/3/2025',     'slug': 'TERCERAFEB'},
    'ligau':       {'name': 'Liga U',       'url': 'https://www.feb.es/competiciones/calendario/ligau/74/2025',         'slug': 'LIGAU'},
}

def get_comp_paths(comp_key: str) -> dict:
    """Devuelve un dict con todas las rutas de ficheros para una competición."""
    if comp_key not in COMPETITIONS:
        raise ValueError(f"Competición '{comp_key}' no soportada. Opciones: {list(COMPETITIONS.keys())}")
    comp = COMPETITIONS[comp_key]
    slug = comp['slug']
    # Primera FEB usa la carpeta raw_data raíz (compatibilidad hacia atrás)
    raw_dir = RAW_API_DIR if comp_key == 'primerafeb' else os.path.join(DATA_DIR, 'raw_data', comp_key)
    os.makedirs(raw_dir, exist_ok=True)
    # El fichero de roles para Primera FEB no lleva sufijo de competición (fichero heredado)
    roles_path = (os.path.join(DATA_DIR, 'PLAYER_ROLES_FINAL_2526.csv')
                  if comp_key == 'primerafeb'
                  else os.path.join(DATA_DIR, f'PLAYER_ROLES_FINAL_{slug}_2526.csv'))
    return {
        'comp_key':  comp_key,
        'comp_name': comp['name'],
        'comp_url':  comp['url'],
        'slug':      slug,
        # Solo Primera FEB escribe en Supabase (las demás no tienen tablas propias)
        'use_db':    comp_key == 'primerafeb',
        'raw_dir':   raw_dir,
        'roster':    os.path.join(DATA_DIR, f'ROSTER_{slug}_2526.csv'),
        'calendario':os.path.join(DATA_DIR, f'CALENDAR_{slug}_2526.csv'),
        'roles':     roles_path,
        'photos':    ARCHIVO_PHOTOS,
        'boxscore':  os.path.join(DATA_DIR, f'BOXSCORE_{slug}_2526.csv'),
        'teamstats': os.path.join(DATA_DIR, f'TEAMSTATS_{slug}_2526.csv'),
        'pbp':       os.path.join(DATA_DIR, f'PLAYBYPLAY_{slug}_2526.csv'),
        'lineups':   os.path.join(DATA_DIR, f'LINEUPS_{slug}_2526.csv'),
    }

# ── Conexión a Supabase (opcional — si no hay DATABASE_URL usa solo CSVs) ─────
from sqlalchemy import create_engine, text as sql_text
_DB_URL = os.environ.get("DATABASE_URL")
_engine  = create_engine(_DB_URL) if _DB_URL else None

def db_ok():
    return _engine is not None

HEADERS_WEB = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}
BASE_URL    = "https://www.feb.es"

TEAM_FIXES = {
    'CLUB OURENSE BALONCESTO': 'CLOUD.GAL OURENSE BALONCESTO',
    'OURENSE BALONCESTO':      'CLOUD.GAL OURENSE BALONCESTO'
}

# ==============================================================================
# FASE 1: DESCARGA DE CALENDARIO Y JSONS RAW
# ==============================================================================
def actualizar_calendario_y_jsons(paths: dict):
    comp_name     = paths['comp_name']
    comp_url      = paths['comp_url']
    archivo_cal   = paths['calendario']
    raw_api_dir   = paths['raw_dir']
    print(f"🗓️ [{comp_name}] Actualizando Calendario Maestro...")
    try:
        r    = requests.get(comp_url, headers=HEADERS_WEB)
        soup = BeautifulSoup(r.text, 'html.parser')
        datos = []
        for col in soup.find_all('div', class_='columna'):
            h1 = col.find('h1', class_='titulo-modulo')
            if not h1: continue
            texto_cabecera = h1.get_text(strip=True)
            match_jornada  = re.search(r'Jornada\s+(\d+)', texto_cabecera, re.IGNORECASE)
            jornada = match_jornada.group(1) if match_jornada else "0"
            tabla = col.find('table')
            if not tabla: continue
            for fila in tabla.find_all('tr'):
                if fila.find('th') or 'LOCAL' in fila.get_text(strip=True).upper(): continue
                a_eq = fila.find_all('a', href=re.compile(r'Equipo\.aspx'))
                a_p  = fila.find('a', href=re.compile(r'Partido\.aspx\?p='))
                if a_p and len(a_eq) >= 2:
                    match_id  = re.search(r'p=(\d+)', a_p['href']).group(1)
                    resultado = a_p.get_text(strip=True)
                    datos.append({"MATCHID": match_id, "ROUND": jornada, "SCORE_STR": resultado})
        df_cal = pd.DataFrame(datos).drop_duplicates(subset=['MATCHID'])
        df_cal.to_csv(archivo_cal, index=False, encoding='utf-8-sig')
        print(f"✅ Calendario actualizado: {len(df_cal)} partidos registrados.")
    except Exception as e:
        print(f"⚠️ Error al actualizar calendario: {e}")
        if not os.path.exists(archivo_cal): return
        df_cal = pd.read_csv(archivo_cal)

    print(f"\n📡 [{comp_name}] Buscando partidos finalizados para descargar JSONs...")
    jugados = df_cal[df_cal['SCORE_STR'].astype(str).str.contains(r'\d+\s*-\s*\d+', regex=True, na=False)]
    session = requests.Session()
    session.headers.update(HEADERS_WEB)
    nuevos_descargados = 0

    for _, partido in jugados.iterrows():
        match_id = str(partido['MATCHID'])
        box_path = os.path.join(raw_api_dir, f"raw_boxscore_{match_id}.json")
        pbp_path = os.path.join(raw_api_dir, f"raw_pbp_{match_id}.json")
        if not os.path.exists(box_path) or not os.path.exists(pbp_path):
            print(f"   ⬇️ Descargando JSONs para el partido {match_id}...")
            url_web = f"https://www.feb.es/competiciones/partido/{match_id}"
            try:
                res_web     = session.get(url_web)
                soup        = BeautifulSoup(res_web.text, 'html.parser')
                token_input = soup.find('input', id='_ctl0_token')
                if not token_input: continue
                token = token_input['value'].strip()
                session.headers.update({"Authorization": f"Bearer {token}", "Origin": BASE_URL,
                                        "Referer": BASE_URL+"/", "Accept": "application/json"})
                base_url_api = "https://intrafeb.feb.es/LiveStats.API/api/v1"
                data_pbp = session.get(f"{base_url_api}/KeyFacts/{match_id}").json()
                with open(pbp_path, "w", encoding="utf-8") as f: json.dump(data_pbp, f, ensure_ascii=False)
                data_box = session.get(f"{base_url_api}/BoxScore/{match_id}").json()
                with open(box_path, "w", encoding="utf-8") as f: json.dump(data_box, f, ensure_ascii=False)
                nuevos_descargados += 1
            except Exception as e:
                print(f"   ⚠️ Error descargando partido {match_id}: {e}")

    print(f"✅ Descarga finalizada. {nuevos_descargados} partidos nuevos almacenados.")

# ==============================================================================
# FASE 2: MOTOR MATEMÁTICO ETL
# ==============================================================================
def to_float(val):
    try:
        if val is None or str(val).strip() == "": return 0.0
        return round(float(str(val).replace(',', '.')), 1)
    except Exception: return 0.0

def safe_div(n, d, default=0.0):
    try:
        den = float(d)
        if den == 0.0 or pd.isna(den): return default
        return round(float(n) / den, 1)
    except Exception: return default

def parse_minutos(time_str):
    if pd.isna(time_str) or not isinstance(time_str, str): return 0.0
    try:
        if ':' in time_str:
            m, s = time_str.split(':')
            return round(float(m) + (float(s) / 60.0), 1)
        return round(float(str(time_str).replace(',', '.')), 1)
    except Exception: return 0.0

def get_5_players_flat(player_ids_set, match_roster_dict):
    pos_order    = {'PG': 1, 'SG': 2, 'SF': 3, 'PF': 4, 'C': 5}
    players_data = []
    for pid in player_ids_set:
        if pd.isna(pid) or str(pid).strip() == "": continue
        pid_str  = str(pid).strip()
        info     = match_roster_dict.get(pid_str, {})
        name     = info.get('PLAYER_NAME', 'Unknown')
        pos      = info.get('POSITION', 'SF')
        rank     = pos_order.get(pos, 6)
        players_data.append((pid_str, name, pos, rank))
    players_data.sort(key=lambda x: x[3])
    while len(players_data) < 5: players_data.append(("", "", "", 7))
    players_data = players_data[:5]
    flat_list = []
    for p in players_data: flat_list.extend([p[0], p[1], p[2]])
    return flat_list

def translate_pbp_action(raw_action, text):
    a = str(raw_action).lower().strip()
    t = str(text).lower().strip()
    if 'subst' in a or 'substitution' in a:
        if 'entra' in t or 'in' in t:  return 'Sub In'
        if 'sale'  in t or 'out' in t: return 'Sub Out'
    if 'tiro de 2' in t or '2pt' in a:
        if 'anotado' in t or 'made' in t or 'm' in a:  return '2PT Made'
        if 'fallado' in t or 'miss' in t or 'miss' in a: return '2PT Missed'
    if 'tiro de 3' in t or '3pt' in a:
        if 'anotado' in t or 'made' in t or 'm' in a:  return '3PT Made'
        if 'fallado' in t or 'miss' in t or 'miss' in a: return '3PT Missed'
    if 'tiro de 1' in t or 'tiro libre' in t or '1pt' in a or 'fthrow' in a:
        if 'anotado' in t or 'made' in t or 'm' in a:  return 'FT Made'
        if 'fallado' in t or 'miss' in t or 'miss' in a: return 'FT Missed'
    if 'turnover'  in a or 'pérdida' in t or 'to' == a: return 'Turnover'
    if 'steal'     in a or 'st' == a  or 'robo' in t:   return 'Steal'
    if 'assist'    in a or 'asistencia' in t:            return 'Assist'
    if 'block'     in a or 'bs' == a  or 'tc' == a or 'tapón' in t: return 'Block'
    if 'foul'      in a or 'falta' in t or 'pf' == a:   return 'Foul'
    if 'rebound'   in a or 'rebote' in t or 'ro' == a or 'rd' == a: return 'Def. Reb'
    return raw_action.title()

def procesar_estadisticas_acumuladas(paths: dict):
    comp_name    = paths['comp_name']
    archivo_ros  = paths['roster']
    archivo_cal  = paths['calendario']
    raw_api_dir  = paths['raw_dir']
    out_boxscore = paths['boxscore']
    out_teamstats= paths['teamstats']
    out_pbp      = paths['pbp']
    out_lineups  = paths['lineups']

    print(f"⏳ [{comp_name}] Iniciando Motor Matemático ETL...")

    if not os.path.exists(archivo_ros):
        pd.DataFrame(columns=['PLAYER_ID','PLAYER','PLAYER_NAME','POSITION']).to_csv(archivo_ros, index=False)

    df_roster    = pd.read_csv(archivo_ros, dtype=str)
    dict_roster_id = {}
    for _, row in df_roster.iterrows():
        if pd.notna(row.get('PLAYER_ID')) and str(row.get('PLAYER_ID','')).strip() != "":
            dict_roster_id[str(row['PLAYER_ID']).strip()] = {
                'PLAYER':      str(row.get('PLAYER',      '')),
                'PLAYER_NAME': str(row.get('PLAYER_NAME', '')),
                'POSITION':    str(row.get('POSITION',    'SF'))
            }

    if not os.path.exists(archivo_cal):
        raise FileNotFoundError(f"Falta el Calendario: {archivo_cal}")
    df_cal        = pd.read_csv(archivo_cal, dtype=str)
    dict_calendar = df_cal.set_index('MATCHID')['ROUND'].to_dict()

    procesados_previos = set()
    if os.path.exists(out_boxscore):
        try:
            df_prev = pd.read_csv(out_boxscore, usecols=['MATCHID'], dtype=str)
            procesados_previos = set(df_prev['MATCHID'].unique())
        except Exception: pass

    archivos_json      = [f for f in os.listdir(raw_api_dir) if f.startswith('raw_boxscore_') and f.endswith('.json')]
    partidos_totales   = set([f.split('_')[2].split('.')[0] for f in archivos_json])
    partidos_a_procesar = partidos_totales - procesados_previos
    print(f"📊 En raw_data: {len(partidos_totales)} | Ya procesados: {len(procesados_previos)} | Nuevos: {len(partidos_a_procesar)}")

    all_boxscores = []; all_teamstats = []; all_pbp = []; all_lineups = []
    errores = 0; procesados_ahora = 0

    for match_id in partidos_a_procesar:
        try:
            box_path = os.path.join(raw_api_dir, f"raw_boxscore_{match_id}.json")
            pbp_path = os.path.join(raw_api_dir, f"raw_pbp_{match_id}.json")
            if not os.path.exists(box_path): continue
            with open(box_path, 'r', encoding='utf-8') as f: data_box = json.load(f)

            match_round = dict_calendar.get(str(match_id), "0")
            teams       = data_box.get('BOXSCORE', {}).get('TEAM', [])
            if len(teams) != 2: continue

            dict_team_locs = {}; dict_team_ids = {}
            pbp_name_resolver  = {}
            local_match_roster = dict_roster_id.copy()

            for i, t in enumerate(teams):
                t_name = str(t.get('name', f'Team_{i}')).upper().strip()
                t_name = TEAM_FIXES.get(t_name, t_name)
                t_id   = str(t.get('id', ''))
                loc    = 'HOME' if str(t.get('isHome', '')).strip().lower() in ['1','true','yes'] else 'AWAY'
                dict_team_locs[t_name] = loc
                dict_team_ids[t_name]  = t_id
                for p in t.get('PLAYER', []):
                    pid      = str(p.get('id', '')).strip()
                    api_name = str(p.get('name', '')).strip()
                    if pid and pid not in local_match_roster:
                        local_match_roster[pid] = {'PLAYER': api_name.title(), 'PLAYER_NAME': api_name.title(), 'POSITION': 'SF'}
                    if pid: pbp_name_resolver[api_name.upper()] = pid

            if list(dict_team_locs.values()).count('HOME') != 1:
                t1, t2 = list(dict_team_locs.keys())[0], list(dict_team_locs.keys())[1]
                dict_team_locs[t1] = 'HOME'; dict_team_locs[t2] = 'AWAY'

            # A) BOXSCORE Y STATS INDIVIDUALES
            match_players = []
            for t in teams:
                t_name = str(t.get('name', '')).upper().strip()
                t_name = TEAM_FIXES.get(t_name, t_name)
                t_id   = dict_team_ids.get(t_name, '')
                loc    = dict_team_locs.get(t_name, 'AWAY')
                for p in t.get('PLAYER', []):
                    pid = str(p.get('id', '')).strip()
                    if not pid: continue
                    player_bruto  = local_match_roster[pid]['PLAYER']
                    player_limpio = local_match_roster[pid]['PLAYER_NAME']
                    min_dec  = parse_minutos(p.get('minFormatted', '00:00'))
                    min_secs = to_float(p.get('min', 0))
                    pts_num  = to_float(p.get('pts', 0))
                    is_starter = 1 if str(p.get('inn', '0')).strip() in ['1','true','*'] else 0
                    if min_dec == 0 and pts_num == 0 and is_starter == 0: continue
                    match_players.append({
                        'MATCHID': match_id, 'ROUND': match_round,
                        'TEAM_ID': t_id, 'TEAM': t_name, 'LOCATION': loc,
                        'PLAYER_ID': pid, 'PLAYER': player_bruto, 'PLAYER_NAME': player_limpio,
                        'IS_STARTER': is_starter, 'MIN': min_dec, 'MIN_SECS': min_secs,
                        'PTS': pts_num,
                        'FGM_2': to_float(p.get('p2m',0)),  'FGA_2': to_float(p.get('p2a',0)),
                        'FGM_3': to_float(p.get('p3m',0)),  'FGA_3': to_float(p.get('p3a',0)),
                        'FGM':   to_float(p.get('fgm',0)),  'FGA':   to_float(p.get('fga',0)),
                        'FTM':   to_float(p.get('p1m',0)),  'FTA':   to_float(p.get('p1a',0)),
                        'ORB':   to_float(p.get('ro', 0)),  'DRB':   to_float(p.get('rd',0)), 'TRB': to_float(p.get('rt',0)),
                        'AST':   to_float(p.get('assist',0)), 'TOV': to_float(p.get('to',0)),
                        'STL':   to_float(p.get('st',0)),   'BLK':   to_float(p.get('bs',0)), 'BLKA': to_float(p.get('tc',0)),
                        'PF':    to_float(p.get('pf',0)),   'PFD':   to_float(p.get('rf',0)),
                        'PIR':   to_float(p.get('val',0)),  'PLUS_MINUS': to_float(p.get('pllss',0))
                    })

            df_match = pd.DataFrame(match_players)
            if df_match.empty: continue

            t_stats = df_match.groupby('TEAM').sum(numeric_only=True).reset_index()
            if len(t_stats) != 2: continue
            t1, t2       = t_stats['TEAM'].iloc[0], t_stats['TEAM'].iloc[1]
            dict_team_totals = t_stats.set_index('TEAM').to_dict('index')

            stats_avanzadas = []
            for _, row in df_match.iterrows():
                mi_t = dict_team_totals.get(row['TEAM'])
                riv_t = dict_team_totals.get(t2 if row['TEAM'] == t1 else t1)
                min_eq = safe_div(mi_t['MIN'], 5.0)
                pts, fgm, fga, fta, tov = row['PTS'], row['FGM'], row['FGA'], row['FTA'], row['TOV']
                opp_poss = riv_t['FGA'] - riv_t['ORB'] + riv_t['TOV'] + (0.44 * riv_t['FTA'])
                stats_avanzadas.append({
                    'TS%':    safe_div(pts*100,   2*(fga+0.44*fta)),
                    'eFG%':   safe_div((fgm+0.5*row['FGM_3'])*100, fga),
                    '3PAr':   safe_div(row['FGA_3']*100, fga),
                    'FTr':    safe_div(fta*100, fga),
                    'PPS_2':  safe_div(row['FGM_2']*2, row['FGA_2']),
                    'PPS_3':  safe_div(row['FGM_3']*3, row['FGA_3']),
                    'FTA_PER_PFD': safe_div(fta, row['PFD']),
                    'ORB%':   safe_div(row['ORB']*min_eq*100, row['MIN']*(mi_t['ORB']+riv_t['DRB'])),
                    'DRB%':   safe_div(row['DRB']*min_eq*100, row['MIN']*(mi_t['DRB']+riv_t['ORB'])),
                    'TRB%':   safe_div(row['TRB']*min_eq*100, row['MIN']*(mi_t['TRB']+riv_t['TRB'])),
                    'AST%':   safe_div(row['AST']*100, (safe_div(row['MIN'],min_eq)*mi_t['FGM'])-fgm),
                    'STL%':   safe_div(row['STL']*min_eq*100, row['MIN']*opp_poss),
                    'BLK%':   safe_div(row['BLK']*min_eq*100, row['MIN']*riv_t['FGA_2']),
                    'TOV%':   safe_div(tov*100, fga+0.44*fta+tov),
                    'USG%':   safe_div((fga+0.44*fta+tov)*min_eq*100, row['MIN']*(mi_t['FGA']+0.44*mi_t['FTA']+mi_t['TOV']))
                })

            df_match_final = pd.concat([df_match.reset_index(drop=True), pd.DataFrame(stats_avanzadas)], axis=1)
            all_boxscores.append(df_match_final)

            # B) TEAM STATS
            team_adv = []
            for tm in [t1, t2]:
                mi_t  = dict_team_totals[tm]
                riv_t = dict_team_totals[t2 if tm == t1 else t1]
                t_id  = dict_team_ids.get(tm, '')
                poss     = mi_t['FGA']  - mi_t['ORB']  + mi_t['TOV']  + (0.44*mi_t['FTA'])
                poss_riv = riv_t['FGA'] - riv_t['ORB'] + riv_t['TOV'] + (0.44*riv_t['FTA'])
                team_adv.append({
                    'MATCHID': match_id, 'ROUND': match_round, 'TEAM_ID': t_id, 'TEAM': tm, 'LOCATION': dict_team_locs.get(tm,'AWAY'),
                    'POSS': round(poss,1), 'PACE': safe_div((poss+poss_riv)*40, 2*(mi_t['MIN']/5.0)),
                    'O_RTG': safe_div(mi_t['PTS']*100, poss), 'D_RTG': safe_div(riv_t['PTS']*100, poss_riv),
                    'NET_RTG': round(safe_div(mi_t['PTS']*100,poss)-safe_div(riv_t['PTS']*100,poss_riv),1),
                    'TS%':  safe_div(mi_t['PTS']*100, 2*(mi_t['FGA']+0.44*mi_t['FTA'])),
                    'eFG%': safe_div((mi_t['FGM']+0.5*mi_t['FGM_3'])*100, mi_t['FGA']),
                    'TOV%': safe_div(mi_t['TOV']*100, mi_t['FGA']+0.44*mi_t['FTA']+mi_t['TOV']),
                    'ORB%': safe_div(mi_t['ORB']*100, mi_t['ORB']+riv_t['DRB']),
                    'DRB%': safe_div(mi_t['DRB']*100, mi_t['DRB']+riv_t['ORB']),
                    'TRB%': safe_div(mi_t['TRB']*100, mi_t['TRB']+riv_t['TRB']),
                    'AST_TOV_RATIO': safe_div(mi_t['AST'], mi_t['TOV']),
                    'FTr':  safe_div(mi_t['FTA']*100, mi_t['FGA'])
                })
            df_team = pd.merge(pd.DataFrame(team_adv), t_stats, on='TEAM')
            df_team = df_team.drop(columns=['IS_STARTER'], errors='ignore')
            cols_base  = ['MATCHID','ROUND','TEAM_ID','TEAM','LOCATION']
            cols_resto = [c for c in df_team.columns if c not in cols_base]
            if 'MIN' in df_team.columns: df_team['MIN'] = df_team['MIN'].round(0).astype(int)
            all_teamstats.append(df_team[cols_base+cols_resto])

            # C) PLAY BY PLAY Y LINEUPS
            if os.path.exists(pbp_path):
                with open(pbp_path, 'r', encoding='utf-8') as f: data_pbp = json.load(f)
                home_on_court = set(df_match[(df_match['LOCATION']=='HOME') & (df_match['IS_STARTER']==1)]['PLAYER_ID'].tolist())
                away_on_court = set(df_match[(df_match['LOCATION']=='AWAY') & (df_match['IS_STARTER']==1)]['PLAYER_ID'].tolist())
                lines = data_pbp.get('PLAYBYPLAY', {}).get('LINES', [])
                if not lines: continue
                df_lines = pd.DataFrame(lines)
                df_lines['quarter'] = pd.to_numeric(df_lines.get('quarter', 1), errors='coerce').fillna(1)
                df_lines['time']    = df_lines.get('time','00:00').fillna('00:00')
                df_lines['SECONDS_REMAINING'] = pd.to_timedelta('00:'+df_lines['time'].astype(str), errors='coerce').dt.total_seconds().fillna(0)
                df_lines['ACTION_TYPE'] = df_lines.apply(lambda x: translate_pbp_action(x.get('action'), x.get('text')), axis=1)
                df_lines['SORT_PRIORITY'] = 3
                df_lines.loc[df_lines['ACTION_TYPE']=='Sub Out','SORT_PRIORITY'] = 1
                df_lines.loc[df_lines['ACTION_TYPE']=='Sub In', 'SORT_PRIORITY'] = 2
                df_lines = df_lines.sort_values(by=['quarter','SECONDS_REMAINING','SORT_PRIORITY'], ascending=[True,False,True]).reset_index(drop=True)
                pbp_records = []; prev_true_action = ""; prev_true_team_id = ""
                for _, row in df_lines.iterrows():
                    action = row['ACTION_TYPE']; text = str(row.get('text',''))
                    action_team = ""; action_team_id = ""; action_team_loc = ""
                    p_id = ""; p_bruto = ""; p_limpio = ""; p_pos = ""
                    match_team = re.search(r'^\((.*?)\)', text)
                    if match_team:
                        team_abbrev = match_team.group(1).upper().strip()
                        for tm in dict_team_locs.keys():
                            if tm.startswith(team_abbrev) or team_abbrev in tm:
                                action_team     = tm
                                action_team_id  = dict_team_ids[tm]
                                action_team_loc = dict_team_locs[tm]
                                break
                    if action == 'Def. Reb':
                        if prev_true_action in ['2PT Missed','3PT Missed','FT Missed']:
                            if action_team_id == prev_true_team_id and action_team_id != "": action = 'Off. Reb'
                        elif prev_true_action == 'Block':
                            action = 'Off. Reb' if action_team_id != prev_true_team_id else 'Def. Reb'
                    raw_id = str(row.get('id','')).strip()
                    if raw_id and raw_id != 'None': p_id = raw_id
                    else:
                        m = re.search(r'^\(.*?\) (.*?)(?::|\s+(?:Substitution|Sustitución|Entra|Sale|in|out))', text, re.IGNORECASE)
                        if m: p_id = pbp_name_resolver.get(m.group(1).upper().strip(), "")
                    if not p_id and 'Reb' in action:
                        p_id = 'TEAM'; p_bruto = 'Team Rebound'; p_limpio = 'Team Rebound'; p_pos = 'TEAM'
                    if p_id and p_id != 'TEAM':
                        info = local_match_roster.get(p_id, {})
                        p_bruto = info.get('PLAYER',''); p_limpio = info.get('PLAYER_NAME',''); p_pos = info.get('POSITION','')
                    if   action == 'Sub Out' and p_id and p_id != 'TEAM':
                        if action_team_loc == 'HOME': home_on_court.discard(p_id)
                        elif action_team_loc == 'AWAY': away_on_court.discard(p_id)
                    elif action == 'Sub In' and p_id and p_id != 'TEAM':
                        if action_team_loc == 'HOME': home_on_court.add(p_id)
                        elif action_team_loc == 'AWAY': away_on_court.add(p_id)
                    if action not in ['Sub In','Sub Out','Timeout','Period']:
                        prev_true_action = action; prev_true_team_id = action_team_id
                    cx = cy = None
                    pos_str = str(row.get('position', row.get('Position','')))
                    if '|' in pos_str:
                        parts = pos_str.split('|')
                        if len(parts) >= 2: cx, cy = parts[0], parts[1]
                    h_flat = get_5_players_flat(home_on_court, local_match_roster)
                    a_flat = get_5_players_flat(away_on_court, local_match_roster)
                    pbp_records.append({
                        'MATCHID': match_id, 'ROUND': match_round,
                        'PERIOD': row['quarter'], 'TIME': row['time'], 'SECONDS_REMAINING': row['SECONDS_REMAINING'],
                        'TEAM_ID': action_team_id, 'ACTION_TEAM': action_team, 'ACTION_TEAM_LOC': action_team_loc,
                        'PLAYER_ID': p_id, 'PLAYER': p_bruto, 'PLAYER_NAME': p_limpio, 'PLAYER_POSITION': p_pos,
                        'ACTION_TYPE': action, 'ACTION_TEXT': text, 'COORD_X': cx, 'COORD_Y': cy,
                        'SCORE_H': row.get('scoreA',''), 'SCORE_A': row.get('scoreB',''),
                        'H1_PLAYER_ID': h_flat[0],  'H1_PLAYER_NAME': h_flat[1],  'H1_PLAYER_POS': h_flat[2],
                        'H2_PLAYER_ID': h_flat[3],  'H2_PLAYER_NAME': h_flat[4],  'H2_PLAYER_POS': h_flat[5],
                        'H3_PLAYER_ID': h_flat[6],  'H3_PLAYER_NAME': h_flat[7],  'H3_PLAYER_POS': h_flat[8],
                        'H4_PLAYER_ID': h_flat[9],  'H4_PLAYER_NAME': h_flat[10], 'H4_PLAYER_POS': h_flat[11],
                        'H5_PLAYER_ID': h_flat[12], 'H5_PLAYER_NAME': h_flat[13], 'H5_PLAYER_POS': h_flat[14],
                        'A1_PLAYER_ID': a_flat[0],  'A1_PLAYER_NAME': a_flat[1],  'A1_PLAYER_POS': a_flat[2],
                        'A2_PLAYER_ID': a_flat[3],  'A2_PLAYER_NAME': a_flat[4],  'A2_PLAYER_POS': a_flat[5],
                        'A3_PLAYER_ID': a_flat[6],  'A3_PLAYER_NAME': a_flat[7],  'A3_PLAYER_POS': a_flat[8],
                        'A4_PLAYER_ID': a_flat[9],  'A4_PLAYER_NAME': a_flat[10], 'A4_PLAYER_POS': a_flat[11],
                        'A5_PLAYER_ID': a_flat[12], 'A5_PLAYER_NAME': a_flat[13], 'A5_PLAYER_POS': a_flat[14],
                    })
                df_pbp_match = pd.DataFrame(pbp_records)
                df_pbp_match['SCORE_H'] = pd.to_numeric(df_pbp_match['SCORE_H'], errors='coerce').ffill().fillna(0)
                df_pbp_match['SCORE_A'] = pd.to_numeric(df_pbp_match['SCORE_A'], errors='coerce').ffill().fillna(0)
                df_pbp_match['SCORE_H_MAX'] = df_pbp_match['SCORE_H'].cummax()
                df_pbp_match['SCORE_A_MAX'] = df_pbp_match['SCORE_A'].cummax()
                df_pbp_match['PTS_H'] = (df_pbp_match['SCORE_H_MAX'] - df_pbp_match['SCORE_H_MAX'].shift(1).fillna(0)).clip(lower=0)
                df_pbp_match['PTS_A'] = (df_pbp_match['SCORE_A_MAX'] - df_pbp_match['SCORE_A_MAX'].shift(1).fillna(0)).clip(lower=0)
                df_pbp_match['H_HASH'] = df_pbp_match[['H1_PLAYER_ID','H2_PLAYER_ID','H3_PLAYER_ID','H4_PLAYER_ID','H5_PLAYER_ID']].astype(str).agg(''.join, axis=1)
                df_pbp_match['A_HASH'] = df_pbp_match[['A1_PLAYER_ID','A2_PLAYER_ID','A3_PLAYER_ID','A4_PLAYER_ID','A5_PLAYER_ID']].astype(str).agg(''.join, axis=1)
                df_pbp_match['LINEUP_CHANGE'] = (df_pbp_match['H_HASH'] != df_pbp_match['H_HASH'].shift()) | \
                                                (df_pbp_match['A_HASH'] != df_pbp_match['A_HASH'].shift()) | \
                                                (df_pbp_match['PERIOD'] != df_pbp_match['PERIOD'].shift())
                df_pbp_match['STINT_ID']   = df_pbp_match['LINEUP_CHANGE'].cumsum()
                df_pbp_match['NEXT_PERIOD'] = df_pbp_match['PERIOD'].shift(-1)
                df_pbp_match['NEXT_TIME']   = df_pbp_match['SECONDS_REMAINING'].shift(-1)
                df_pbp_match['DURATION']    = np.where(
                    df_pbp_match['PERIOD'] == df_pbp_match['NEXT_PERIOD'],
                    df_pbp_match['SECONDS_REMAINING'] - df_pbp_match['NEXT_TIME'],
                    df_pbp_match['SECONDS_REMAINING']
                ).clip(0)
                cols_pbp = [
                    'MATCHID','ROUND','PERIOD','TIME','SECONDS_REMAINING','TEAM_ID','ACTION_TEAM','ACTION_TEAM_LOC',
                    'PLAYER_ID','PLAYER','PLAYER_NAME','PLAYER_POSITION','ACTION_TYPE','ACTION_TEXT',
                    'COORD_X','COORD_Y','SCORE_H','SCORE_A',
                    'H1_PLAYER_ID','H1_PLAYER_NAME','H1_PLAYER_POS','H2_PLAYER_ID','H2_PLAYER_NAME','H2_PLAYER_POS',
                    'H3_PLAYER_ID','H3_PLAYER_NAME','H3_PLAYER_POS','H4_PLAYER_ID','H4_PLAYER_NAME','H4_PLAYER_POS',
                    'H5_PLAYER_ID','H5_PLAYER_NAME','H5_PLAYER_POS',
                    'A1_PLAYER_ID','A1_PLAYER_NAME','A1_PLAYER_POS','A2_PLAYER_ID','A2_PLAYER_NAME','A2_PLAYER_POS',
                    'A3_PLAYER_ID','A3_PLAYER_NAME','A3_PLAYER_POS','A4_PLAYER_ID','A4_PLAYER_NAME','A4_PLAYER_POS',
                    'A5_PLAYER_ID','A5_PLAYER_NAME','A5_PLAYER_POS','STINT_ID'
                ]
                all_pbp.append(df_pbp_match[cols_pbp].copy())
                for loc_val, p_id_cols, p_name_cols, p_pos_cols in [
                    ('HOME', ['H1_PLAYER_ID','H2_PLAYER_ID','H3_PLAYER_ID','H4_PLAYER_ID','H5_PLAYER_ID'],
                             ['H1_PLAYER_NAME','H2_PLAYER_NAME','H3_PLAYER_NAME','H4_PLAYER_NAME','H5_PLAYER_NAME'],
                             ['H1_PLAYER_POS','H2_PLAYER_POS','H3_PLAYER_POS','H4_PLAYER_POS','H5_PLAYER_POS']),
                    ('AWAY', ['A1_PLAYER_ID','A2_PLAYER_ID','A3_PLAYER_ID','A4_PLAYER_ID','A5_PLAYER_ID'],
                             ['A1_PLAYER_NAME','A2_PLAYER_NAME','A3_PLAYER_NAME','A4_PLAYER_NAME','A5_PLAYER_NAME'],
                             ['A1_PLAYER_POS','A2_PLAYER_POS','A3_PLAYER_POS','A4_PLAYER_POS','A5_PLAYER_POS'])]:
                    tm = next((t_n for t_n, l in dict_team_locs.items() if l == loc_val), None)
                    if not tm: continue
                    t_id = dict_team_ids.get(tm, "")
                    stints = df_pbp_match.groupby('STINT_ID').agg(
                        {**{c:'first' for c in p_id_cols+p_name_cols+p_pos_cols}, 'DURATION':'sum','PTS_H':'sum','PTS_A':'sum'}
                    )
                    for _, s_row in stints.iterrows():
                        pid1,pid2,pid3,pid4,pid5 = s_row[p_id_cols[0]],s_row[p_id_cols[1]],s_row[p_id_cols[2]],s_row[p_id_cols[3]],s_row[p_id_cols[4]]
                        if "" in [pid1,pid2,pid3,pid4,pid5]: continue
                        duration = s_row['DURATION']
                        if duration == 0 and s_row['PTS_H'] == 0 and s_row['PTS_A'] == 0: continue
                        pts_for = s_row['PTS_H'] if loc_val == 'HOME' else s_row['PTS_A']
                        pts_agt = s_row['PTS_A'] if loc_val == 'HOME' else s_row['PTS_H']
                        all_lineups.append({
                            'MATCHID': match_id, 'ROUND': match_round, 'TEAM_ID': t_id, 'TEAM': tm, 'LOCATION': loc_val,
                            'P1_ID': pid1, 'P1_NAME': s_row[p_name_cols[0]], 'P1_POS': s_row[p_pos_cols[0]],
                            'P2_ID': pid2, 'P2_NAME': s_row[p_name_cols[1]], 'P2_POS': s_row[p_pos_cols[1]],
                            'P3_ID': pid3, 'P3_NAME': s_row[p_name_cols[2]], 'P3_POS': s_row[p_pos_cols[2]],
                            'P4_ID': pid4, 'P4_NAME': s_row[p_name_cols[3]], 'P4_POS': s_row[p_pos_cols[3]],
                            'P5_ID': pid5, 'P5_NAME': s_row[p_name_cols[4]], 'P5_POS': s_row[p_pos_cols[4]],
                            'MINUTES': round(duration/60.0,1), 'SECONDS': duration,
                            'PTS_FOR': pts_for, 'PTS_AGAINST': pts_agt, 'PLUS_MINUS': pts_for-pts_agt,
                        })
            procesados_ahora += 1
        except Exception as e:
            errores += 1
            print(f"⚠️ Error partido {match_id}: {e}")
            continue

    # E) APPEND ACUMULATIVO
    use_db = paths.get('use_db', False)

    def append_and_save(new_data_list, filepath, tabla=None, conflict_cols=None):
        if not new_data_list: return
        df_new = pd.concat(new_data_list, ignore_index=True) \
                 if isinstance(new_data_list[0], pd.DataFrame) \
                 else pd.DataFrame(new_data_list)

        # ── Escritura en Supabase (solo para competiciones que usan BD) ────────
        if use_db and db_ok() and tabla:
            try:
                df_db = df_new.copy()
                df_db.columns = df_db.columns.str.lower()
                staging = f"{tabla}_staging"
                df_db.to_sql(staging, _engine, if_exists='replace',
                             index=False, method='multi', chunksize=200)
                if conflict_cols:
                    conflict_str = ", ".join(conflict_cols)
                    with _engine.connect() as conn:
                        conn.execute(sql_text(f"""
                            INSERT INTO {tabla}
                            SELECT * FROM {staging}
                            ON CONFLICT ({conflict_str}) DO NOTHING;
                            DROP TABLE IF EXISTS {staging};
                        """))
                        conn.commit()
                else:
                    with _engine.connect() as conn:
                        conn.execute(sql_text(f"""
                            INSERT INTO {tabla} SELECT * FROM {staging};
                            DROP TABLE IF EXISTS {staging};
                        """))
                        conn.commit()
                print(f"  ✅ BD actualizada: tabla '{tabla}'")
            except Exception as e:
                print(f"  ⚠️ Error escribiendo en BD ({tabla}): {e}")

        # ── Escritura CSV (backup) ────────────────────────────────────────────
        if os.path.exists(filepath):
            try:
                df_old   = pd.read_csv(filepath, dtype=str)
                df_final = pd.concat([df_old, df_new.astype(str)], ignore_index=True)
            except Exception:
                df_final = df_new
        else:
            df_final = df_new
        df_final['ROUND_NUM'] = pd.to_numeric(
            df_final['ROUND'], errors='coerce').fillna(0).astype(int)
        df_final = df_final.sort_values(['ROUND_NUM','MATCHID'])\
                           .drop(columns=['ROUND_NUM'])
        df_final.to_csv(filepath, index=False, encoding='utf-8-sig',
                        float_format='%.1f')

    if all_boxscores:
        append_and_save(all_boxscores, out_boxscore,
                        tabla='boxscore',
                        conflict_cols=['match_id', 'player_id'])
    if all_teamstats:
        append_and_save(all_teamstats, out_teamstats,
                        tabla='teamstats',
                        conflict_cols=['match_id', 'team_id'])
    if all_pbp:
        append_and_save(all_pbp, out_pbp,
                        tabla='pbp')
    if all_lineups:
        df_lu = pd.DataFrame(all_lineups)
        agrupadores = ['MATCHID','ROUND','TEAM_ID','TEAM','LOCATION',
                       'P1_ID','P1_NAME','P1_POS','P2_ID','P2_NAME','P2_POS',
                       'P3_ID','P3_NAME','P3_POS','P4_ID','P4_NAME','P4_POS','P5_ID','P5_NAME','P5_POS']
        df_lu_final = df_lu.groupby(agrupadores).sum(numeric_only=True).reset_index()
        df_lu_final['PTS_FOR_PER40'] = (df_lu_final['PTS_FOR']    *2400/df_lu_final['SECONDS'].replace(0,np.nan)).round(1).fillna(0)
        df_lu_final['PTS_AGT_PER40'] = (df_lu_final['PTS_AGAINST']*2400/df_lu_final['SECONDS'].replace(0,np.nan)).round(1).fillna(0)
        df_lu_final['NET_PER40']     = (df_lu_final['PLUS_MINUS'] *2400/df_lu_final['SECONDS'].replace(0,np.nan)).round(1).fillna(0)
        append_and_save([df_lu_final], out_lineups,
                        tabla='lineups',
                        conflict_cols=['match_id', 'team_id',
                                       'p1_id', 'p2_id', 'p3_id', 'p4_id', 'p5_id'])

    return procesados_ahora, errores

# ==============================================================================
# FASE 3: GENERACIÓN DEL ROSTER MAESTRO
# ==============================================================================
def generar_roster_maestro(paths: dict):
    """
    Construye ROSTER_{SLUG}_2526.csv combinando:
      - BOXSCORE: stats de temporada por jugador (totales + per game)
      - PLAYER_NAMES_DICT.json: foto, posición clásica, POS_ORDER
      - PLAYER_ROLES_FINAL_2526.csv: rol K-Means, métricas avanzadas
    Se regenera COMPLETO en cada ejecución para mantenerlo actualizado.
    """
    comp_name    = paths['comp_name']
    out_boxscore = paths['boxscore']
    archivo_ros  = paths['roster']
    archivo_roles= paths['roles']
    archivo_phot = paths['photos']

    if not os.path.exists(out_boxscore):
        print(f"⚠️ [{comp_name}] No existe el BOXSCORE maestro. Saltando generación de Roster.")
        return

    print(f"\n👤 [{comp_name}] Generando Roster Maestro...")

    # ── 1. BASE: agregar BOXSCORE por jugador ──────────────────────────────────
    df_box = pd.read_csv(out_boxscore)
    df_box['PLAYER_ID'] = df_box['PLAYER_ID'].astype(str).str.replace('.0','',regex=False).str.strip()
    df_box['TEAM_ID']   = df_box['TEAM_ID'].astype(str).str.replace('.0','',regex=False).str.strip()
    df_box['TEAM']      = df_box['TEAM'].replace(TEAM_FIXES)

    num_cols = ['IS_STARTER','MIN','MIN_SECS','PTS','FGM_2','FGA_2','FGM_3','FGA_3',
                'FGM','FGA','FTM','FTA','ORB','DRB','TRB','AST','TOV','STL','BLK',
                'BLKA','PF','PFD','PIR','PLUS_MINUS']
    for c in num_cols:
        if c in df_box.columns:
            df_box[c] = pd.to_numeric(df_box[c], errors='coerce').fillna(0)

    agg = df_box.groupby(['PLAYER_ID','PLAYER_NAME','TEAM_ID','TEAM']).agg(
        GP            = ('MATCHID','nunique'),
        GS            = ('IS_STARTER','sum'),
        MIN_TOTAL     = ('MIN','sum'),
        MIN_SECS_TOTAL= ('MIN_SECS','sum'),
        PTS           = ('PTS','sum'),
        FGM_2         = ('FGM_2','sum'), FGA_2 = ('FGA_2','sum'),
        FGM_3         = ('FGM_3','sum'), FGA_3 = ('FGA_3','sum'),
        FGM           = ('FGM','sum'),   FGA   = ('FGA','sum'),
        FTM           = ('FTM','sum'),   FTA   = ('FTA','sum'),
        ORB           = ('ORB','sum'),   DRB   = ('DRB','sum'),   TRB     = ('TRB','sum'),
        AST           = ('AST','sum'),   TOV   = ('TOV','sum'),
        STL           = ('STL','sum'),   BLK   = ('BLK','sum'),   BLKA    = ('BLKA','sum'),
        PF            = ('PF','sum'),    PFD   = ('PFD','sum'),
        PIR           = ('PIR','sum'),   PLUS_MINUS = ('PLUS_MINUS','sum'),
    ).reset_index()

    # Per game
    for c in ['PTS','FGM_2','FGA_2','FGM_3','FGA_3','FGM','FGA','FTM','FTA',
              'ORB','DRB','TRB','AST','TOV','STL','BLK','BLKA','PF','PFD','PIR','PLUS_MINUS']:
        agg[f'{c}_PG'] = (agg[c] / agg['GP'].replace(0, np.nan)).round(1).fillna(0)

    agg['MIN_PG'] = (agg['MIN_TOTAL'] / agg['GP'].replace(0, np.nan)).round(1).fillna(0)

    # Shooting %
    agg['FG2_PCT'] = np.where(agg['FGA_2']>0, (agg['FGM_2']/agg['FGA_2']*100).round(1), 0.0)
    agg['FG3_PCT'] = np.where(agg['FGA_3']>0, (agg['FGM_3']/agg['FGA_3']*100).round(1), 0.0)
    agg['FT_PCT']  = np.where(agg['FTA'] >0,  (agg['FTM'] /agg['FTA'] *100).round(1), 0.0)

    # Advanced (season totals — más fiables que el promedio de partidos)
    ts_denom       = 2*(agg['FGA'] + 0.44*agg['FTA'])
    agg['TS_PCT']  = np.where(ts_denom>0, (agg['PTS']/ts_denom*100).round(1), 0.0)
    agg['EFG_PCT'] = np.where(agg['FGA']>0, ((agg['FGM']+0.5*agg['FGM_3'])/agg['FGA']*100).round(1), 0.0)
    agg['3PAr']    = np.where(agg['FGA']>0, (agg['FGA_3']/agg['FGA']*100).round(1), 0.0)
    agg['FTr']     = np.where(agg['FGA']>0, (agg['FTA']/agg['FGA']*100).round(1), 0.0)
    tov_denom      = agg['FGA'] + 0.44*agg['FTA'] + agg['TOV']
    agg['TOV_PCT'] = np.where(tov_denom>0, (agg['TOV']/tov_denom*100).round(1), 0.0)

    # Game Score per game
    agg['GMSC_PG'] = ((
        agg['PTS'] + 0.4*agg['FGM'] - 0.7*agg['FGA']
        - 0.4*(agg['FTA']-agg['FTM']) + 0.7*agg['ORB'] + 0.3*agg['DRB']
        + agg['STL'] + 0.7*agg['AST'] + 0.7*agg['BLK']
        - 0.4*agg['PF'] - agg['TOV']
    ) / agg['GP'].replace(0, np.nan)).round(1).fillna(0)

    # ── 2. ENRIQUECER: PLAYER_NAMES_DICT.json ─────────────────────────────────
    dict_photos = {}
    if os.path.exists(archivo_phot):
        try:
            with open(archivo_phot, 'r', encoding='utf-8') as f:
                raw = json.load(f)
            for pid, info in raw.items():
                pid_clean = str(pid).strip().replace('.0','')
                dict_photos[pid_clean] = {
                    'PHOTO_URL': info.get('PHOTO_URL', f"https://imagenes.feb.es/Foto.aspx?c={pid_clean}"),
                    'POSITION':  info.get('POSITION', ''),
                    'POS_ORDER': info.get('POS_ORDER', 6),
                }
        except Exception as e:
            print(f"  ⚠️ No se pudo leer PLAYER_NAMES_DICT.json: {e}")

    agg['PHOTO_URL'] = agg['PLAYER_ID'].map(lambda pid: dict_photos.get(pid, {}).get('PHOTO_URL', f"https://imagenes.feb.es/Foto.aspx?c={pid}"))
    agg['POSITION']  = agg['PLAYER_ID'].map(lambda pid: dict_photos.get(pid, {}).get('POSITION', ''))
    agg['POS_ORDER'] = agg['PLAYER_ID'].map(lambda pid: dict_photos.get(pid, {}).get('POS_ORDER', 6))

    # ── 3. ENRIQUECER: fichero de roles ───────────────────────────────────────
    if os.path.exists(archivo_roles):
        try:
            df_roles = pd.read_csv(archivo_roles)
            df_roles['PLAYER_ID'] = df_roles['PLAYER_ID'].astype(str).str.replace('.0','',regex=False).str.strip()
            df_roles = df_roles.rename(columns={
                'ROLE_NAME': 'ROLE_NAME',
                'POSITION':  'POSITION_ROLE',   # renombramos para no colisionar
                'eFG%':  'ROLE_EFG_PCT',
                'TS%':   'ROLE_TS_PCT',
                'TOV%':  'ROLE_TOV_PCT',
                'ORB%':  'ROLE_ORB_PCT',
                'FTr':   'ROLE_FTr',
                'USG%':  'ROLE_USG_PCT',
            })
            cols_roles = ['PLAYER_ID','ROLE_NAME','POSITION_ROLE','ROLE_EFG_PCT','ROLE_TS_PCT',
                          'ROLE_TOV_PCT','ROLE_ORB_PCT','ROLE_FTr','ROLE_USG_PCT']
            cols_roles = [c for c in cols_roles if c in df_roles.columns]
            agg = pd.merge(agg, df_roles[cols_roles], on='PLAYER_ID', how='left')
            # Rellenar POSITION con POSITION_ROLE si el JSON no tenía datos
            agg['POSITION'] = np.where(
                agg['POSITION'].isna() | (agg['POSITION'].astype(str).str.strip() == ''),
                agg.get('POSITION_ROLE', ''),
                agg['POSITION']
            )
            agg = agg.drop(columns=['POSITION_ROLE'], errors='ignore')
        except Exception as e:
            print(f"  ⚠️ No se pudo leer {archivo_roles}: {e}")
    else:
        agg['ROLE_NAME'] = 'N/A'

    # ── 4. ORDENAR COLUMNAS Y GUARDAR ──────────────────────────────────────────
    final_cols = [
        # Identificación
        'PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'TEAM',
        # Perfil
        'POSITION', 'POS_ORDER', 'ROLE_NAME', 'PHOTO_URL',
        # Participación
        'GP', 'GS', 'MIN_PG',
        # Per game tradicionales
        'PTS_PG', 'ORB_PG', 'DRB_PG', 'TRB_PG', 'AST_PG',
        'STL_PG', 'BLK_PG', 'BLKA_PG', 'TOV_PG',
        'PF_PG', 'PFD_PG', 'PLUS_MINUS_PG', 'PIR_PG',
        # Tiros por partido
        'FGM_2_PG', 'FGA_2_PG', 'FG2_PCT',
        'FGM_3_PG', 'FGA_3_PG', 'FG3_PCT',
        'FTM_PG',   'FTA_PG',   'FT_PCT',
        # Avanzadas de temporada
        'TS_PCT', 'EFG_PCT', '3PAr', 'FTr', 'TOV_PCT', 'GMSC_PG',
        # Avanzadas del rol (del modelo K-Means)
        'ROLE_EFG_PCT', 'ROLE_TS_PCT', 'ROLE_TOV_PCT',
        'ROLE_ORB_PCT', 'ROLE_FTr', 'ROLE_USG_PCT',
        # Totales de temporada (útil para lookups)
        'PTS', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PIR',
        'FGM_2', 'FGA_2', 'FGM_3', 'FGA_3', 'FTM', 'FTA',
        'MIN_TOTAL', 'MIN_SECS_TOTAL',
    ]
    # Solo incluir columnas que existan
    final_cols = [c for c in final_cols if c in agg.columns]
    df_roster_final = agg[final_cols].sort_values(by=['TEAM','POS_ORDER','PTS_PG'], ascending=[True,True,False])
    df_roster_final.to_csv(archivo_ros, index=False, encoding='utf-8-sig', float_format='%.1f')
    print(f"✅ [{comp_name}] Roster Maestro generado: {len(df_roster_final)} jugadores | {len(final_cols)} columnas → {archivo_ros}")

# ==============================================================================
# EJECUCIÓN PRINCIPAL
# ==============================================================================
if __name__ == "__main__":
    print("🚀 INICIANDO ROBOT ETL MULTI-COMPETICIÓN...")
    for comp_key in COMPETITIONS:
        print(f"\n{'='*60}")
        print(f"🏀 Procesando: {COMPETITIONS[comp_key]['name']}")
        print(f"{'='*60}")
        paths = get_comp_paths(comp_key)
        actualizar_calendario_y_jsons(paths)
        try:
            procesados, fails = procesar_estadisticas_acumuladas(paths)
            print(f"\n✅ ETL [{paths['comp_name']}] COMPLETADO. Nuevos: {procesados} | Errores: {fails}")
        except Exception as e:
            print(f"\n❌ Error Crítico en ETL [{paths['comp_name']}]:\n{traceback.format_exc()}")

        try:
            generar_roster_maestro(paths)
        except Exception as e:
            print(f"\n❌ Error en generación del Roster [{paths['comp_name']}]:\n{traceback.format_exc()}")

    print("\n🏁 PROCESO COMPLETO MULTI-COMPETICIÓN.")
