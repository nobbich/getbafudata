# -*- coding: utf-8 -*-
"""
Created on Tue Mar  4 15:52:02 2025

@author: norbsute
"""

import requests
import re

API_BASE_URL = "https://environment.ld.admin.ch/foen/hydro"


def fetch_station_data(station_id):
    """Holt die Daten aus drei unterschiedlichen Endpunkten und kombiniert die Antworten."""
    response_parts = {}

    urls = {
        "geometry": f"{API_BASE_URL}/station/{station_id}/geometry",
        "station": f"{API_BASE_URL}/station/{station_id}",
        "observation": f"{API_BASE_URL}/river/observation/{station_id}",
    }

    for key, url in urls.items():
        response = requests.get(url)
        print("url: " + url + "response: " + str(response))
        if response.status_code == 200:
            response.encoding = "utf-8"
            response_parts[key] = response.text
        else:
            print(
                f"Warnung: {key} für Station {station_id} nicht gefunden. (Status: {response.status_code})"
            )

    return response_parts


def parse_station_data(station_id, responses):
    """Extrahiert Koordinaten, Stationsname und Messwerte aus den API-Antworten."""
    station_elem = {"id": station_id}

    # 🔹 Geometry extrahieren (Koordinaten)
    if "geometry" in responses:
        match = re.search(
            r'"POINT\(([\d.]+) ([\d.]+)\)"', responses["geometry"]
        )
        if match:
            station_elem["longitude"] = match.group(1)
            station_elem["latitude"] = match.group(2)

    # 🔹 Station Name extrahieren
    if "station" in responses:
        match = re.search(
            r'<http://schema.org/name>\s*"([^"]+)"', responses["station"]
        )
        if match:
            station_elem["name"] = match.group(1)

    # 🔹 Messwerte aus Observation extrahieren
    if "observation" in responses:
        # print(responses)
        measurement_time_match = re.search(
            r'<https://environment.ld.admin.ch/foen/hydro/dimension/measurementTime>\s*"([^"]+)"\^\^<http://www.w3.org/2001/XMLSchema#dateTime>',
            responses["observation"],
        )
        water_level_match = re.search(
            r"<https://environment.ld.admin.ch/foen/hydro/dimension/waterLevel>\s*([\d.]+)",
            responses["observation"],
        )
        discharge_match = re.search(
            r"<https://environment.ld.admin.ch/foen/hydro/dimension/discharge>\s*([\d.]+)",
            responses["observation"],
        )

        station_elem["measurement_time"] = (
            measurement_time_match.group(1)
            if measurement_time_match
            else "N/A"
        )
        station_elem["water_level"] = (
            water_level_match.group(1) if water_level_match else "N/A"
        )
        station_elem["discharge"] = (
            discharge_match.group(1) if discharge_match else "N/A"
        )

    return station_elem


def generate_xml(station_ids):
    """Holt Daten für mehrere Stationen und kombiniert sie zu einer strukturierten Liste."""
    all_stations = []

    for station_id in station_ids:
        responses = fetch_station_data(station_id)
        station_data = parse_station_data(station_id, responses)
        all_stations.append(station_data)

    return all_stations


# Testaufruf
# station_ids = [2135, 2403, 2639]  # Beispiel-Stationen
station_ids = [2135]
result = generate_xml(station_ids)
print(result)
