# scripts/init_db.py
# Ejecutar UNA SOLA VEZ para migrar los datos históricos de los CSVs a Supabase
import os
import json
import pandas as pd
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("❌ No se encontró DATABASE_URL en las variables de entorno")

engine = create_engine(DATABASE_URL)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DIR  = os.path.join(DATA_DIR, "raw_data")

TEAM_FIXES = {
    'CLUB OURENSE BALONCESTO': 'CLOUD.GAL OURENSE BALONCESTO',
    'OURENSE BALONCESTO':      'CLOUD.GAL OURENSE BALONCESTO'
}

def migrar_calendario():
    print("📅 Migrando calendario...")
    ruta = os.path.join(DATA_DIR, "CALENDAR_PRIMERAFEB_2526.csv")
    if not os.path.exists(ruta):
        print("  ⚠️ No encontrado, saltando.")
        return
    df = pd.read_csv(ruta, dtype=str)
    df.columns = df.columns.str.upper()
    df = df.rename(columns={'MATCHID': 'match_id', 'ROUND': 'round', 'SCORE_STR': 'score_str'})
    df['match_id'] = pd.to_numeric(df['match_id'], errors='coerce').dropna().astype(int)
    df['round']    = pd.to_numeric(df['round'],    errors='coerce').fillna(0).astype(int)
    df = df[['match_id', 'round', 'score_str']].drop_duplicates(subset=['match_id'])
    df.to_sql('matches', engine, if_exists='append', index=False, method='multi')
    print(f"  ✅ {len(df)} partidos migrados.")

def migrar_boxscore():
    print("📊 Migrando boxscore...")
    ruta = os.path.join(DATA_DIR, "BOXSCORE_PRIMERAFEB_2526.csv")
    if not os.path.exists(ruta):
        print("  ⚠️ No encontrado, saltando.")
        return
    df = pd.read_csv(ruta)
    df.columns = df.columns.str.lower()
    df['team'] = df['team'].replace({k.lower(): v for k, v in TEAM_FIXES.items()})
    # Renombrar columnas para que coincidan con el esquema
    rename = {
        'matchid': 'match_id', 'min_secs': 'min_secs',
        'fgm_2': 'fgm_2', 'fga_2': 'fga_2',
        'fgm_3': 'fgm_3', 'fga_3': 'fga_3',
        'plus_minus': 'plus_minus', 'ts%': 'ts_pct',
        'efg%': 'efg_pct', 'tov%': 'tov_pct',
        'orb%': 'orb_pct', 'drb%': 'drb_pct', 'trb%': 'trb_pct',
        'ast%': 'ast_pct', 'stl%': 'stl_pct', 'blk%': 'blk_pct',
        'usg%': 'usg_pct', '3par': 'par3',
        'pps_2': 'pps_2', 'pps_3': 'pps_3'
    }
    df = df.rename(columns=rename)
    # Quitar columnas que no están en el esquema
    cols_validas = [
        'match_id','round','team_id','team','location','player_id','player','player_name',
        'is_starter','min','min_secs','pts','pir','plus_minus',
        'fgm_2','fga_2','fgm_3','fga_3','fgm','fga','ftm','fta',
        'orb','drb','trb','ast','tov','stl','blk','blka','pf','pfd',
        'ts_pct','efg_pct','tov_pct','orb_pct','drb_pct','trb_pct',
        'ast_pct','stl_pct','blk_pct','usg_pct','par3','ftr','pps_2','pps_3'
    ]
    cols_presentes = [c for c in cols_validas if c in df.columns]
    df = df[cols_presentes].drop_duplicates(subset=['match_id','player_id'])
    df.to_sql('boxscore', engine, if_exists='append', index=False,
              method='multi', chunksize=200)
    print(f"  ✅ {len(df)} filas migradas.")

def migrar_lineups():
    print("🏀 Migrando lineups...")
    ruta = os.path.join(DATA_DIR, "LINEUPS_PRIMERAFEB_2526.csv")
    if not os.path.exists(ruta):
        print("  ⚠️ No encontrado, saltando.")
        return
    df = pd.read_csv(ruta)
    df.columns = df.columns.str.lower()
    df['team'] = df['team'].replace(TEAM_FIXES)
    rename = {
        'matchid': 'match_id',
        'pts_for_per40': 'pts_for_per40',
        'pts_agt_per40': 'pts_agt_per40',
        'net_per40':     'net_per40'
    }
    df = df.rename(columns=rename)
    cols_validas = [
        'match_id','round','team_id','team','location',
        'p1_id','p1_name','p1_pos','p2_id','p2_name','p2_pos',
        'p3_id','p3_name','p3_pos','p4_id','p4_name','p4_pos',
        'p5_id','p5_name','p5_pos',
        'minutes','seconds','pts_for','pts_against','plus_minus',
        'pts_for_per40','pts_agt_per40','net_per40'
    ]
    cols_presentes = [c for c in cols_validas if c in df.columns]
    df = df[cols_presentes]
    df.to_sql('lineups', engine, if_exists='append', index=False,
              method='multi', chunksize=200)
    print(f"  ✅ {len(df)} filas migradas.")

def migrar_teamstats():
    print("📈 Migrando teamstats...")
    ruta = os.path.join(DATA_DIR, "TEAMSTATS_PRIMERAFEB_2526.csv")
    if not os.path.exists(ruta):
        print("  ⚠️ No encontrado, saltando.")
        return
    df = pd.read_csv(ruta)
    df.columns = df.columns.str.lower()
    df['team'] = df['team'].replace(TEAM_FIXES)
    rename = {
        'matchid':      'match_id',
        'ast_tov_ratio':'ast_tov',
        'net_rtg':      'net_rtg',
        'o_rtg':        'o_rtg',
        'd_rtg':        'd_rtg'
    }
    df = df.rename(columns=rename)
    cols_validas = [
        'match_id','round','team_id','team','location',
        'poss','pace','o_rtg','d_rtg','net_rtg',
        'ts_pct','efg_pct','tov_pct','orb_pct','drb_pct','trb_pct',
        'ast_tov','ftr','pts','orb','drb','trb','ast','tov','stl','blk'
    ]
    cols_presentes = [c for c in cols_validas if c in df.columns]
    df = df[cols_presentes].drop_duplicates(subset=['match_id','team_id'])
    df.to_sql('teamstats', engine, if_exists='append', index=False,
              method='multi', chunksize=200)
    print(f"  ✅ {len(df)} filas migradas.")

def migrar_raw_jsons():
    print("📦 Migrando JSONs raw...")
    if not os.path.exists(RAW_DIR):
        print("  ⚠️ Carpeta raw_data no encontrada, saltando.")
        return
    records = []
    for fname in os.listdir(RAW_DIR):
        if not fname.endswith('.json'): continue
        partes = fname.replace('.json', '').split('_')
        if len(partes) < 3: continue
        tipo = partes[1]        # 'boxscore' o 'pbp'
        try:
            mid = int(partes[2])
        except: continue
        with open(os.path.join(RAW_DIR, fname), 'r', encoding='utf-8') as f:
            contenido = f.read()
        records.append({'match_id': mid, 'tipo': tipo, 'contenido': contenido})
    if records:
        pd.DataFrame(records).to_sql('raw_json', engine, if_exists='append',
                                     index=False, method='multi', chunksize=50)
    print(f"  ✅ {len(records)} JSONs migrados.")

if __name__ == "__main__":
    print("🚀 Iniciando migración histórica a Supabase...\n")
    migrar_calendario()
    migrar_boxscore()
    migrar_lineups()
    migrar_teamstats()
    migrar_raw_jsons()
    print("\n🏁 Migración completada.")
