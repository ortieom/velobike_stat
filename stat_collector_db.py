# -*- coding: utf-8 -*-
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from tempfile import NamedTemporaryFile
import datetime as dt
import sqlite3 as sl
import requests
import warnings
import shutil
import json
import csv

# get response
warnings.simplefilter('ignore', InsecureRequestWarning)
r = requests.get('http://velobike.ru/ajax/parkings/', verify=False).text
r = r.replace('false', '0')
r = r.replace('true', '1')
r = json.loads(r)

# all stations mentioned in response
stations_in_response = []
for station in r['Items']:
    stations_in_response.append(station['Id'])


def prepare_file_for_deltas(response, file):  # utility file
    create_new = False  # flag, true if file is new and nothing recorded

    try:  # inspecting
        with open(file, "r", encoding="utf-8", newline='') as csvfile:
            reader = csv.reader(csvfile, quotechar='"')
            if len(list(reader)) == 0:  # empty file
                create_new = True
            for row in reader:   # row in utility file
                if len(row) == 0 or len(row) == 1:  # confused string
                    create_new = True
    except FileNotFoundError:
        create_new = True

    if create_new:  # creating if needed
        with open(file, "w", encoding="utf-8", newline='') as csvfile:
            writer = csv.writer(csvfile, quotechar='"')

            writer.writerow(['Id', dt.datetime.now().strftime("%d.%m.%y_%H:%M")])  # header

            for row in response:
                writer.writerow([row['Id'], row["FreeOrdinaryPlaces"], row["FreeElectricPlaces"]])


def record_deltas(response, file, db):
    tempfile = NamedTemporaryFile('w+t', encoding="utf-8", newline='', delete=False)

    current_time = dt.datetime.now().strftime("%d.%m.%y_%H:%M")

    conn = sl.connect(db)
    cursor = conn.cursor()

    unused_stations_in_response = stations_in_response.copy()

    with open(file, 'r', encoding="utf-8", newline='') as csvFile, tempfile:
        reader = csv.reader(csvFile, delimiter=',', quotechar='"')
        writer = csv.writer(tempfile, delimiter=',', quotechar='"')

        for row in reader:  # row in utility file
            try:
                unused_stations_in_response.pop(unused_stations_in_response.index(row[0]))
                station_info = response[stations_in_response.index(row[0])]  # info for station by index of velobike_id

                if row[-1] != '-':  # delta for previous value
                    delta_ordinary = int(row[-2]) - station_info["FreeOrdinaryPlaces"]
                    delta_electro = int(row[-1]) - station_info["FreeElectricPlaces"]
                else:
                    delta_ordinary = delta_electro = 0

                cursor.execute('INSERT INTO collected_data '
                               '(velobike_id, timestamp, ordinarybikes_available, electrobikes_available) '
                               'VALUES (?, ?, ?, ?)',
                               (row[0], current_time, delta_ordinary, delta_electro))

                row[-1] = station_info["FreeElectricPlaces"]
                row[-2] = station_info["FreeOrdinaryPlaces"]

                writer.writerow(row)

            except ValueError:
                if str(row[0]) == 'Id':  # updating header in utility file
                    row[-1] = current_time
                else:  # not mentioned in response station (present in utility file)
                    row[-1] = row[-2] = '-'

                    cursor.execute('INSERT INTO collected_data '
                                   '(velobike_id, timestamp, ordinarybikes_available, electrobikes_available) '
                                   'VALUES (?, ?, NULL, NULL)',
                                   (row[0], current_time))

                writer.writerow(row)

        for new_station in unused_stations_in_response:  # stations without previous records in utility file
            station_info = response[stations_in_response.index(new_station)]

            cursor.execute('INSERT INTO collected_data '
                           '(velobike_id, timestamp, ordinarybikes_available, electrobikes_available) '
                           'VALUES (?, ?, 0, 0)',
                           (station_info['Id'], current_time))

            new_station_row = [station_info['Id'],
                               station_info["FreeOrdinaryPlaces"],  station_info["FreeElectricPlaces"]]
            writer.writerow(new_station_row)

    shutil.move(tempfile.name, file)

    conn.commit()
    conn.close()


prepare_file_for_deltas(r['Items'], 'temp_records.csv')  # creating utility file on start if missing
record_deltas(r['Items'], 'temp_records.csv', 'velobike.db')  # adding deltas
