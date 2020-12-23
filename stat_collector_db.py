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


def prepare_file_for_deltas(response, file, db):  # utility file
    create_new = False  # flag, true if file is new and nothing recorded

    try:
        with open(file, "r", encoding="utf-8", newline='') as csvfile:
            reader = csv.reader(csvfile, quotechar='"')
            if len(list(reader)) == 0:  # empty file
                create_new = True
            for row in reader:   # row in utility file
                if len(row) == 0 or len(row) == 1:  # confused string
                    create_new = True
    except FileNotFoundError:
        create_new = True

    if create_new:
        with open(file, "w", encoding="utf-8", newline='') as csvfile:
            writer = csv.writer(csvfile, quotechar='"')

            writer.writerow(['Id', dt.datetime.now().strftime("%H:%M")])  # header

            for row in response:
                total_free_places_now = row["FreeElectricPlaces"] + row["FreeOrdinaryPlaces"]
                writer.writerow([row['Id'], total_free_places_now])

    # preparing db
    conn = sl.connect(db)
    cursor = conn.cursor()
    cursor.execute("SELECT velobike_id FROM collected_data ")
    recorded_ids = cursor.fetchall()
    if len(recorded_ids) == 0:
        for row in response:
            cursor.execute("INSERT INTO collected_data VALUES (?)", (row['Id'],))
        conn.commit()
    conn.close()


def record_deltas(response, file, db):
    tempfile = NamedTemporaryFile('w+t', encoding="utf-8", newline='', delete=False)

    current_time = dt.datetime.now().strftime('%H:%M')

    conn = sl.connect(db)
    cursor = conn.cursor()
    try:
        cursor.execute(f"ALTER TABLE collected_data ADD COLUMN '{current_time}' INTEGER")
    except sl.DatabaseError:
        pass

    unused_stations_in_response = stations_in_response.copy()

    with open(file, 'r', encoding="utf-8", newline='') as csvFile, tempfile:
        reader = csv.reader(csvFile, delimiter=',', quotechar='"')
        writer = csv.writer(tempfile, delimiter=',', quotechar='"')

        for row in reader:  # row in utility file
            try:
                unused_stations_in_response.pop(unused_stations_in_response.index(row[0]))
                station_info = response[stations_in_response.index(row[0])]  # info for station by index of velobike_id
                total_free_places_now = station_info["FreeElectricPlaces"] + station_info["FreeOrdinaryPlaces"]

                if row[-1] != '-':  # delta for previous value
                    delta = int(row[-1]) - total_free_places_now
                else:
                    delta = 0
                cursor.execute(f"UPDATE collected_data SET '{current_time}' = '{delta}' \n"
                               f"WHERE velobike_id = '{row[0]}'")

                row[-1] = total_free_places_now
                writer.writerow(row)

            except ValueError:
                if str(row[0]) == 'Id':  # updating header in utility file
                    row[-1] = current_time
                else:  # not mentioned in response station (present in utility file)
                    row[-1] = '-'

                    cursor.execute(f"UPDATE collected_data SET '{current_time}' = NULL \n"
                                   f"WHERE velobike_id = '{row[0]}'")

                writer.writerow(row)

        for new_station in unused_stations_in_response:  # stations without previous records in utility file
            station_info = response[stations_in_response.index(new_station)]

            cursor.execute(f"INSERT INTO collected_data (velobike_id, '{current_time}') VALUES (?, ?)",
                           (station_info['Id'], 0))

            total_free_places_now = station_info["FreeElectricPlaces"] + station_info["FreeOrdinaryPlaces"]
            new_station_row = [station_info['Id'], total_free_places_now]
            writer.writerow(new_station_row)

    shutil.move(tempfile.name, file)

    conn.commit()
    conn.close()


prepare_file_for_deltas(r['Items'], 'temp_records.csv', 'velobike.db')  # keeping utility file actual on start
record_deltas(r['Items'], 'temp_records.csv', 'velobike.db')  # adding deltas
