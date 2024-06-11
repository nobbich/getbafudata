#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 11 21:37:56 2024

@author: nsuter
"""

import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET

# get data from station page
url = "https://www.hydrodaten.admin.ch/de/seen-und-fluesse/stationen-und-daten/2639"
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')

# extract table content
table = soup.find('tbody')
rows = table.find_all('tr')

# convert to xml
root = ET.Element('root')
for row in rows:
    columns = row.find_all('td')
    entry = ET.SubElement(root, 'entry')
    
    if len(columns) == 3:
        date = ET.SubElement(entry, 'date')
        date_text = columns[0].text.replace("Letzter Messwert\n", "").strip()
        date.text = date_text
        
        discharge = ET.SubElement(entry, 'flow_m3s')
        discharge.text = columns[1].text.strip()
        
        water_level = ET.SubElement(entry, 'water_level_ASL')
        water_level.text = columns[2].text.strip()
    else:
        name = ET.SubElement(entry, 'name')
        name.text = columns[0].text.strip()
        
        value1 = ET.SubElement(entry, 'value1')
        value1.text = columns[1].text.strip()
        
        value2 = ET.SubElement(entry, 'value2')
        value2.text = columns[2].text.strip()

# XML-Dokument als String
xml_data = ET.tostring(root, encoding='utf8', method='xml')

print(xml_data)
