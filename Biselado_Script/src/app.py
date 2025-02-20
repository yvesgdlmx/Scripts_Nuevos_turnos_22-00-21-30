import csv
import re
from datetime import datetime, timedelta
import mysql.connector

def extract_hour(field_name):
    hour_match = re.search(r"(\d{1,2}):(\d{2})", field_name)
    if hour_match:
        return f"{hour_match.group(1)}:{hour_match.group(2)}"
    return ""

def extract_num(field_name):
    num_match = re.search(r"^(\d+)", field_name)
    return num_match.group(1) if num_match else None

def extract_date(field_name, extracted_hour):
    parts = field_name.split('-')
    if len(parts) >= 2:
        day = int(parts[1])
        now = datetime.now()
        current_year = now.year
        current_month = now.month
        # Si es temprano y el día extraído es mayor que hoy, se resta un día
        if now.hour < 4 and day > now.day:
            extracted_date = datetime(current_year, current_month, day) - timedelta(days=1)
        else:
            extracted_date = datetime(current_year, current_month, day)
        # Si la hora extraída es "23:30", se ajusta restando un día adicional
        if extracted_hour == "23:30":
            extracted_date -= timedelta(days=1)
        return extracted_date.strftime("%Y-%m-%d")
    return None

def clean_value(value):
    return None if value in ['N/A', 'inf%'] else value

def clean_percentage(value):
    if value in ['N/A', 'inf%']:
        return None
    if value.endswith('%'):
        return float(value.strip('%')) / 100
    return float(value)

def get_existing_hits(cursor, name, fecha, hour):
    query = """
    SELECT hits FROM biselados WHERE name = %s AND fecha = %s AND hour = %s
    """
    cursor.execute(query, (name, fecha, hour))
    result = cursor.fetchone()
    return result[0] if result else None

def delete_existing_record(cursor, name, fecha, hour):
    query = """
    DELETE FROM biselados WHERE name = %s AND fecha = %s AND hour = %s
    """
    cursor.execute(query, (name, fecha, hour))

def is_valid_time_for_processing(extracted_hour, extracted_date):
    now = datetime.now()
    try:
        extracted_datetime = datetime.strptime(f"{extracted_date} {extracted_hour}", "%Y-%m-%d %H:%M")
    except ValueError:
        return False

    # Excepción para registros con hora "23:00": se aceptan a partir de las 23:50.
    if extracted_hour == "23:00":
        if now.time() >= datetime.strptime("23:50", "%H:%M").time():
            return True

    limit_time = now - timedelta(hours=1)
    return extracted_datetime <= limit_time

def procesar_archivo(input_file):
    start_processing = False
    data = []
    try:
        connection = mysql.connector.connect(
           host='autorack.proxy.rlwy.net',
            port=22723,
            user='root',
            password='zsulNCCrYFSfBqIxwwIXIKqLQKFJWwbw',
            database='railway'
        )
        if connection.is_connected():
            print("Conexión establecida exitosamente.")
        cursor = connection.cursor()
        with open(input_file, 'r') as original_file:
            reader = csv.reader(original_file, delimiter='\t')
            for row in reader:
                if row and row[0] == 'Key':
                    start_processing = True
                    continue
                if start_processing and row and row[0].strip():
                    name_field = row[0]
                    extracted_hour = extract_hour(name_field)
                    
                    # Convertir la hora extraída a minutos totales para filtrar según el turno
                    try:
                        h, m = map(int, extracted_hour.split(':'))
                    except ValueError:
                        continue
                    total_minutes = h * 60 + m
                    if "NVO" in input_file:
                        # Turno nocturno: se aceptan registros entre 22:00 (1320 min) y 06:00 (360 min)
                        if not (total_minutes >= 1320 or total_minutes <= 300):
                            continue
                    else:
                        # Turno diurno: se aceptan registros entre 06:30 (390 min) y 21:30 (1290 min)
                        if not (total_minutes >= 390 and total_minutes <= 1290):
                            continue

                    extracted_date = extract_date(name_field, extracted_hour)
                    if not extracted_date or not is_valid_time_for_processing(extracted_hour, extracted_date):
                        continue
                    extracted_num = extract_num(name_field)
                    hits_index = 3  # Ajusta este índice según la posición real de "hits" en tu archivo
                    print(f"Procesando fila: {row}")
                    print(f"Hits extraídos: {row[hits_index]}")
                    try:
                        current_hits = int(row[hits_index])
                    except ValueError:
                        print(f"Error al convertir hits a entero: {row[hits_index]}")
                        continue
                    existing_hits = get_existing_hits(cursor, name_field, extracted_date, extracted_hour)
                    if existing_hits is None or (current_hits is not None and current_hits > existing_hits):
                        if existing_hits is not None:
                            delete_existing_record(cursor, name_field, extracted_date, extracted_hour)
                        # Se inserta la fecha en la posición 1; se adjuntan la hora y el número extraído al final
                        row.insert(1, extracted_date)
                        row.append(extracted_hour)
                        row.append(extracted_num)
                        row[2] = clean_value(row[2])      # mean
                        row[3] = clean_value(row[3])      # median
                        row[4] = current_hits             # hits
                        row[5] = clean_percentage(row[5]) # multi
                        row[6] = clean_value(row[6])      # inf fails (nuevo campo)
                        row[7] = clean_value(row[7])      # shortest
                        row[8] = clean_value(row[8])      # longest
                        row[9] = clean_value(row[9])      # total
                        row[10] = clean_value(row[10])    # stddev
                        data.append(row)
        print(f"Número de filas para insertar: {len(data)}")
        sql_insert = """
        INSERT INTO biselados (name, fecha, mean, median, hits, multi, `inf fails`, shortest, longest, total, stddev, hour, num)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(sql_insert, data)
        connection.commit()
        print("Datos insertados exitosamente.")
    except mysql.connector.Error as err:
        print("Error al ejecutar el comando SQL:", err)
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()
    print("Carga de datos completada.")

# Seleccionar el archivo a procesar según la hora actual
current_time = datetime.now().time()
hora_noche = datetime.strptime("22:00", "%H:%M").time()
hora_manana = datetime.strptime("06:30", "%H:%M").time()

if current_time >= hora_noche or current_time < hora_manana:
    input_file = 'I:/VISION/scantotals_BISNVO.auto.tab'
else:
    input_file = 'I:/VISION/scantotals_YVES5.auto.tab'

print("Archivo seleccionado:", input_file)
procesar_archivo(input_file)