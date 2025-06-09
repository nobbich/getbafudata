#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Erstellt am: Mon Jun  9 23:55:20 2025
Autor: nsuter
"""

import requests
import pandas as pd
from urllib.parse import unquote
from SPARQLWrapper import SPARQLWrapper, JSON

# ==== Schritt 1: Stationsliste abfragen ====
query = """
PREFIX schema: <http://schema.org/>

SELECT ?station ?id ?name ?waterbody
FROM <https://lindas.admin.ch/foen/hydro>
WHERE {
  ?station schema:identifier ?id ;
           schema:name ?name .
  OPTIONAL { ?station schema:containedInPlace ?waterbody . }
  FILTER CONTAINS(STR(?station), "/foen/hydro/station/")
}
ORDER BY ?id
"""

endpoint = "https://lindas.admin.ch/query"
sparql = SPARQLWrapper(endpoint)
sparql.setQuery(query)
sparql.setReturnFormat(JSON)
results = sparql.query().convert()

stations = []
for r in results["results"]["bindings"]:
    sid = r["id"]["value"]
    name = r["name"]["value"]
    waterbody_url = r.get("waterbody", {}).get("value", None)
    waterbody = unquote(waterbody_url.rsplit("/", 1)[-1]) if waterbody_url else None
    stations.append({
        "id": sid,
        "name": name,
        "waterbody": waterbody,
        "waterbody_url": waterbody_url
    })

# ==== Schritt 2: Geometrie holen ====
def get_geometry(station_id):
    url = f"https://environment.ld.admin.ch/foen/hydro/station/{station_id}/geometry"
    r = requests.get(url, headers={"Accept": "application/ld+json"})
    if r.status_code == 200:
        data = r.json()
        graph = data.get('@graph', [])
        for entry in graph:
            wkt_field = entry.get('http://www.opengis.net/ont/geosparql#asWKT')
            if wkt_field:
                if isinstance(wkt_field, list) and wkt_field:
                    return wkt_field[0].get('@value')
                elif isinstance(wkt_field, dict):
                    return wkt_field.get('@value')
    return None

# ==== Schritt 3: Typ des waterbody abfragen ====
def get_waterbody_type(waterbody_url):
    r = requests.get(waterbody_url, headers={"Accept": "application/ld+json"})
    if r.status_code == 200:
        data = r.json()
        graph = data.get('@graph', [])
        for entry in graph:
            additional_type = entry.get('http://schema.org/additionalType')
            if additional_type:
                if isinstance(additional_type, dict):
                    return additional_type.get('@id', '').rsplit('/', 1)[-1]
                elif isinstance(additional_type, list):
                    return ", ".join(a.get('@id', '').rsplit('/', 1)[-1] for a in additional_type if '@id' in a)
    return None

# ==== Alles zusammensetzen ====
for station in stations:
    station["wkt"] = get_geometry(station["id"])
    if station["waterbody_url"]:
        station["waterbody_type"] = get_waterbody_type(station["waterbody_url"])
    else:
        station["waterbody_type"] = None
    del station["waterbody_url"]  # optional: URL nicht in CSV

# ==== DataFrame schreiben ====
df = pd.DataFrame(stations)
print(df)
df.to_csv("stations_mit_geometry_und_typ.csv", index=False)
print("CSV mit Geometrien und Waterbody-Typen erstellt.")
