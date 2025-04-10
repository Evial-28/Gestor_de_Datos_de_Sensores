# Lee archivos CSV de reportes, procesa los datos y los inserta en la BD MySQL.
# Calcula el valor 'flow_per_hour' usando SQL después de la inserción.

import os
import pandas as pd
from datetime import datetime
import mysql.connector
import re
import traceback

# --- Configuración ---
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "1234", # Asegúrate que sea la correcta
    "database": "labiot_data_sensed"
}
CSV_DIRECTORY = r"C:\Users\erika\Desktop\SERVICIO SOCIAL - BD\PYTHON\Reportes desde gmail" # Ruta a los CSV
# Mapeo de patrones de nombre de archivo a ID de sensor en la BD
SENSOR_MAPPING = {
    r'report-d-swm-02.*': 2,
    r'report-d-swm-03.*': 3,
    r'report-d-swm-04.*': 4,
    r'report-d-swm-05.*': 5,
    r'report-pv-sw01.*': 1,
}

# --- Funciones de Base de Datos ---

def connect_to_database():
    """Establece conexión con la base de datos MySQL."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        print("Conexión a BD establecida.")
        return conn
    except mysql.connector.Error as err:
        print(f"❌ Error de conexión BD: {err}")
        return None

def insert_data_to_table(sensor_id, date, water_flow_value, total_pulse, last_pulse, battery, connection):
    """Inserta una fila de datos de sensor en la tabla sensor_data."""
    if connection is None: return
    cursor = None
    try:
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO sensor_data
        (sensor_id, time, water_flow_value, total_pulse, last_pulse, battery)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        data_tuple = (sensor_id, date, water_flow_value, total_pulse, last_pulse, battery)
        cursor.execute(insert_query, data_tuple)
        connection.commit()
    except mysql.connector.Error as err:
        print(f"❌ Error insertando en sensor_data: {err} - Datos: {data_tuple}")
        # traceback.print_exc() # Descomentar para traza completa
    finally:
        if cursor: cursor.close()

def update_flow_per_hour(connection):
    """Calcula y actualiza la columna flow_per_hour usando SQL (Requiere MySQL 8.0+)."""
    if connection is None or not connection.is_connected():
        print("❌ Conexión BD no disponible (update_flow_per_hour).")
        return
    cursor = None
    try:
        cursor = connection.cursor()
        # Consulta SQL que usa LAG() para calcular la diferencia con el valor anterior
        update_query = """
        WITH SensorDataWithPrev AS (
            SELECT
                id,
                water_flow_value,
                LAG(water_flow_value, 1, NULL) OVER (PARTITION BY sensor_id ORDER BY time) AS prev_water_flow_value
            FROM sensor_data
        )
        UPDATE sensor_data sd
        JOIN SensorDataWithPrev sdwp ON sd.id = sdwp.id
        SET sd.flow_per_hour =
            CASE
                WHEN sdwp.prev_water_flow_value IS NOT NULL AND sdwp.water_flow_value IS NOT NULL THEN
                    ROUND(sdwp.water_flow_value - sdwp.prev_water_flow_value, 2) -- Redondeado
                ELSE NULL
            END;
        """
        print("⚙️ Calculando y actualizando flow_per_hour vía SQL...")
        cursor.execute(update_query)
        connection.commit()
        print(f"✅ {cursor.rowcount} filas actualizadas en flow_per_hour.")
    except mysql.connector.Error as err:
        print(f"❌ Error SQL al actualizar flow_per_hour: {err}")
        print("  (Asegúrate de usar MySQL 8.0+ o MariaDB 10.2+ para la función LAG)")
        # traceback.print_exc()
    finally:
        if cursor: cursor.close()

def insert_processed_file(file_name, connection):
    """Registra un archivo como procesado en la tabla processed_files."""
    if connection is None: return
    cursor = None
    try:
        cursor = connection.cursor()
        insert_query = "INSERT INTO processed_files (file_name) VALUES (%s)"
        cursor.execute(insert_query, (file_name,))
        connection.commit()
        # print(f"Archivo {file_name} registrado.") # Menos verboso
    except mysql.connector.Error as err:
        if err.errno == mysql.connector.errorcode.ER_DUP_ENTRY: # Error esperado si ya existe
             pass # print(f"Archivo {file_name} ya estaba registrado.")
        else:
            print(f"❌ Error insertando en processed_files: {err}")
    finally:
        if cursor: cursor.close()

def is_file_processed(file_name, connection):
    """Verifica si un archivo ya fue registrado como procesado."""
    if connection is None: return False # Asumir no procesado si no hay conexión
    cursor = None
    is_processed = False
    try:
        cursor = connection.cursor()
        query = "SELECT 1 FROM processed_files WHERE file_name = %s LIMIT 1" # Más eficiente
        cursor.execute(query, (file_name,))
        is_processed = cursor.fetchone() is not None
    except mysql.connector.Error as err:
        print(f"❌ Error verificando archivo procesado {file_name}: {err}")
    finally:
        if cursor: cursor.close()
    return is_processed

def get_sensor_id(sensor_id, connection):
    """Verifica si un ID de sensor existe en la tabla 'sensors'."""
    if connection is None: return False
    cursor = None
    exists = False
    try:
        cursor = connection.cursor()
        query = "SELECT 1 FROM sensors WHERE id = %s LIMIT 1"
        cursor.execute(query, (sensor_id,))
        exists = cursor.fetchone() is not None
    except mysql.connector.Error as err:
        print(f"❌ Error buscando sensor ID {sensor_id}: {err}")
    finally:
        if cursor: cursor.close()
    return exists

# --- Flujo Principal de Procesamiento ---
if __name__ == "__main__":
    print("--- Iniciando Carga de Datos CSV a BD ---")
    conn = connect_to_database()
    if conn is None:
        exit(1) # Salir si no hay conexión

    processed_files_in_run = 0 # Contar archivos nuevos en esta ejecución

    # Iterar sobre archivos en el directorio, ordenados por nombre
    try:
        # Usar list comprehension para filtrar solo CSV y luego ordenar
        csv_files = sorted([f for f in os.listdir(CSV_DIRECTORY) if f.lower().endswith('.csv')])
    except FileNotFoundError:
        print(f"❌ Error: Directorio CSV no encontrado en '{CSV_DIRECTORY}'. Abortando.")
        exit(1)

    print(f"Encontrados {len(csv_files)} archivos CSV en {CSV_DIRECTORY}")

    for file_name in csv_files:
        file_path = os.path.join(CSV_DIRECTORY, file_name)

        # Omitir si ya fue procesado
        if is_file_processed(file_name, conn):
            continue

        print(f"\n-> Procesando: {file_name}")

        # Validar y leer CSV
        try:
            if os.path.getsize(file_path) == 0:
                print("  ⚠️ Archivo vacío. Marcando como procesado.")
                insert_processed_file(file_name, conn)
                continue
            df = pd.read_csv(file_path)
            if df.empty:
                print("  ⚠️ Archivo CSV sin datos. Marcando como procesado.")
                insert_processed_file(file_name, conn)
                continue
        except Exception as read_err:
            print(f"  ❌ Error leyendo CSV {file_name}: {read_err}. Omitiendo.")
            continue # Saltar al siguiente archivo

        # Limpiar columnas sin nombre (comunes en algunos CSV)
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

        # Identificar sensor_id
        sensor_id = None
        for pattern, id_val in SENSOR_MAPPING.items():
            if re.match(pattern, file_name, re.IGNORECASE): # Ignorar mayúsculas/minúsculas
                sensor_id = id_val
                break
        if sensor_id is None: print(f"  ⚠️ Sensor no mapeado para {file_name}. Omitiendo."); continue
        if not get_sensor_id(sensor_id, conn): print(f"  ⚠️ Sensor ID {sensor_id} no existe en BD. Omitiendo."); continue

        # Preparar columna de fecha y ordenar
        try:
            df['time_dt'] = pd.to_datetime(df['time'], format="%a, %d %b %Y %H:%M:%S", errors='coerce')
            df = df.dropna(subset=['time_dt'])
            df = df.sort_values(by='time_dt')
            if df.empty: print(f"  ⚠️ Sin fechas válidas en {file_name}. Marcando como procesado."); insert_processed_file(file_name, conn); continue
        except KeyError: print(f"  ⚠️ Columna 'time' no encontrada en {file_name}. Omitiendo."); continue
        except Exception as sort_err: print(f"  ❌ Error preparando fecha en {file_name}: {sort_err}. Omitiendo."); continue

        # Insertar filas
        rows_inserted_count = 0
        for index, row in df.iterrows():
            try:
                fecha_sql = row['time_dt'].strftime("%Y-%m-%d %H:%M:%S")
                wfv = row.get('Water Flow Value'); total_p = row.get('Total Pulse')
                last_p = row.get('Last Pulse'); batt = row.get('Battery')
                # Conversión segura a tipos numéricos, manejando None
                wfv = None if pd.isna(wfv) else float(wfv)
                total_p = None if pd.isna(total_p) else int(float(total_p))
                last_p = None if pd.isna(last_p) else int(float(last_p))
                batt = None if pd.isna(batt) else float(batt)

                insert_data_to_table(sensor_id, fecha_sql, wfv, total_p, last_p, batt, conn)
                rows_inserted_count += 1
            except Exception as row_err:
                print(f"    ❌ Error procesando fila {index} de {file_name}: {row_err}")

        print(f"  -> {rows_inserted_count} filas insertadas.")
        insert_processed_file(file_name, conn) # Marcar archivo como procesado
        processed_files_in_run += rows_inserted_count # Contar filas en lugar de archivos para la actualización

    # --- Actualizar flow_per_hour al final si se insertaron filas ---
    if processed_files_in_run > 0:
        print("\n--- Ejecutando actualización final de flow_per_hour ---")
        update_flow_per_hour(conn)
    else:
        print("\n--- No se insertaron filas nuevas, omitiendo actualización de flow_per_hour ---")

    # Cerrar conexión
    if conn and conn.is_connected():
        conn.close()
        print("Conexión a BD cerrada.")
    print("--- Proceso de Carga Finalizado ---")