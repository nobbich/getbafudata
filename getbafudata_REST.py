# -*- coding: utf-8 -*-
"""
Created on Fri Mar  7 13:56:39 2025

@author: norbsute
"""

import requests
import re
import psycopg2
# from datetime import datetime
# from airflow import DAG
# from airflow.operators.python import PythonOperator

API_BASE_URL = "https://environment.ld.admin.ch/foen/hydro"

def fetch_station_data(station_id):
    """ Holt die Daten aus drei unterschiedlichen Endpunkten und kombiniert die Antworten. """
    response_parts = {}

    urls = {
        "geometry": f"{API_BASE_URL}/station/{station_id}/geometry",
        "station": f"{API_BASE_URL}/station/{station_id}",
        "observation": f"{API_BASE_URL}/river/observation/{station_id}"
    }

    for key, url in urls.items():
        response = requests.get(url)
        if response.status_code == 200:
            response.encoding = 'utf-8'
            response_parts[key] = response.text
        else:
            print(f"⚠️ Warnung: {key} für Station {station_id} nicht gefunden. (Status: {response.status_code})")

    return response_parts

def parse_station_data(station_id, responses):
    """ Extrahiert Koordinaten, Stationsname und Messwerte aus den API-Antworten. """
    station_elem = {"id": station_id}

    # 🔹 Geometry extrahieren (Koordinaten)
    if "geometry" in responses:
        match = re.search(r'"POINT\(([\d.]+) ([\d.]+)\)"', responses["geometry"])
        if match:
            station_elem["longitude"] = match.group(1)
            station_elem["latitude"] = match.group(2)

    # 🔹 Station Name extrahieren
    if "station" in responses:
        match = re.search(r'<http://schema.org/name>\s*"([^"]+)"', responses["station"])
        if match:
            station_elem["name"] = match.group(1)

    # 🔹 Messwerte aus Observation extrahieren
    if "observation" in responses:
        measurement_time_match = re.search(
            r'<https://environment.ld.admin.ch/foen/hydro/dimension/measurementTime>\s*"([^"]+)"\^\^<http://www.w3.org/2001/XMLSchema#dateTime>',
            responses["observation"]
        )
        water_level_match = re.search(r'<https://environment.ld.admin.ch/foen/hydro/dimension/waterLevel>\s*([\d.]+)', responses["observation"])
        discharge_match = re.search(r'<https://environment.ld.admin.ch/foen/hydro/dimension/discharge>\s*([\d.]+)', responses["observation"])

        station_elem["measurement_time"] = measurement_time_match.group(1) if measurement_time_match else "N/A"
        station_elem["water_level"] = water_level_match.group(1) if water_level_match else "N/A"
        station_elem["discharge"] = discharge_match.group(1) if discharge_match else "N/A"

    return station_elem

def insert_measurement_data(data):
    """ Fügt die Messdaten in die PostgreSQL-Datenbank ein oder aktualisiert sie. """
    conn = psycopg2.connect(
        dbname="bafu",
        user="airflow",
        password="airflow",
        host="subdn1042",
        port="5432"
    )
    cur = conn.cursor()

    for entry in data:
        if 'longitude' not in entry or 'latitude' not in entry:
            print(f"Fehlende Koordinaten für {entry.get('name', 'Unbekannte Station')}, wird übersprungen.")
            continue  # Überspringe Einträge ohne Koordinaten

        # WKT (Well-Known-Text) für den Punkt erstellen
        point_wkt = f"POINT({entry['longitude']} {entry['latitude']})"

        query = """
            INSERT INTO measurement_data (station_id, name, measurement_time, water_level, discharge, location)
            VALUES (%s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326))
            ON CONFLICT (station_id, measurement_time)
            DO UPDATE SET
                name = EXCLUDED.name,
                water_level = EXCLUDED.water_level,
                discharge = EXCLUDED.discharge,
                location = EXCLUDED.location;
        """
        cur.execute(query, (
            entry['id'],
            entry['name'],
            entry['measurement_time'],
            entry['water_level'],
            entry['discharge'],
            point_wkt  # Hier als einzelnes Argument übergeben
        ))

    conn.commit()
    cur.close()
    conn.close()
    
def fetch_and_insert_data():
    """ Holt die Station-Daten von der API und fügt sie in die DB ein. """
    station_ids = [2135, 2403, 2639]  # Beispiel-Stationen, die IDs können nach Bedarf geändert werden
    all_stations = []

    for station_id in station_ids:
        responses = fetch_station_data(station_id)
        station_data = parse_station_data(station_id, responses)
        all_stations.append(station_data)

    insert_measurement_data(all_stations)

# # Airflow DAG
# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2025, 3, 5),
#     'retries': 1,
# }

# with DAG('fetch_specific_set_bafu_data_and_store_it_to_pg_table',
#          default_args=default_args,
#          # schedule_interval='@daily',  # Hier kannst du das Intervall nach Bedarf ändern
#          schedule_interval="2,7,12-59/10 * * * *",
#          max_active_runs=1,
#          catchup=False) as dag:

    # fetch_and_insert_data_task = PythonOperator(
    #     task_id='fetch_and_insert_measurement_data',
    #     python_callable=fetch_and_insert_data,
    # )

    # fetch_and_insert_data_task
    
# for running without Airflow:
fetch_and_insert_data()
