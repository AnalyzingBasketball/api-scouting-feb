import os
import pandas as pd
# Importamos tus funciones directamente de tu main.py para no repetir código
from main import (
    extraer_diccionario_logos,
    construir_calendario_maestro,
    extraer_maestro_jugadores,
    extraer_partido_api,
    limpiar_y_avanzadas,
    DATA_DIR
)

print("🤖 Iniciando actualización automática de la base de datos...")

print("1. Actualizando Diccionario de Logos...")
extraer_diccionario_logos()

print("2. Actualizando Calendario Maestro...")
construir_calendario_maestro()

print("3. Actualizando Roster (Maestro de Jugadores)...")
extraer_maestro_jugadores()

print("4. Buscando partidos jugados y descargando datos crudos...")
archivo_calendario = os.path.join(DATA_DIR, "calendario_maestro_primerafeb_2025.csv")

if os.path.exists(archivo_calendario):
    df_cal = pd.read_csv(archivo_calendario)
    # Filtramos solo los partidos que tienen un resultado (ej. "80 - 75")
    jugados = df_cal[df_cal['resultado'].astype(str).str.contains(r'\d+\s*-\s*\d+', regex=True, na=False)]

    for _, partido in jugados.iterrows():
        match_id = str(partido['match_id'])
        archivo_pbp = os.path.join(DATA_DIR, f'pbp_{match_id}.csv')
        
        # Solo descargamos si NO lo tenemos ya guardado en GitHub
        if not os.path.exists(archivo_pbp):
            print(f"   ⬇️ Descargando partido nuevo: {partido['equipo_local']} vs {partido['equipo_visitante']} (ID: {match_id})")
            exito = extraer_partido_api(match_id)
            if exito:
                try:
                    # Hacemos la limpieza avanzada y guardamos los CSVs limpios
                    limpiar_y_avanzadas(match_id, partido['equipo_local'], partido['equipo_visitante'], partido['jornada'])
                    print(f"   ✅ Partido {match_id} procesado y guardado con éxito.")
                except Exception as e:
                    print(f"   ⚠️ Error procesando el partido {match_id}: {e}")
        else:
            # Si ya existe, lo ignoramos para ir súper rápido
            pass
else:
    print("❌ Error: No se encontró el calendario maestro.")

print("🎉 Proceso del Robot finalizado.")
