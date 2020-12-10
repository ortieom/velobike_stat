# -*- coding: utf-8 -*-
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from tempfile import NamedTemporaryFile
import datetime as dt
import requests
import warnings
import shutil
import json
import csv

# get response
warnings.simplefilter('ignore', InsecureRequestWarning)
r = requests.get('https://velobike.ru/ajax/parkings/', verify=False).text
r = r.replace('false', '0')
r = r.replace('true', '1')
r = json.loads(r)

# print(r)

# all stations mentioned in response
stations_in_response = []
for station in r['Items']:
    stations_in_response.append(station['Id'])


def check_if_updates(response, file):
    stations_in_records = {}  # dict of added stations and their locations
    need_update = False  # suppose that there is nothing to rewrite

    try:
        with open(file, encoding="utf-8", newline='') as csvfile:
            reader = csv.reader(csvfile, quotechar='"')
            for row in reader:
                stations_in_records[str(row[0])] = str(row[1])  # id: location
    except FileNotFoundError:
        open(file, 'w')
        need_update = True

    # comparing stations in response and records
    for station in response:
        if station['Id'] not in list(stations_in_records.keys()) \
                or str(station['Position']) != stations_in_records[station['Id']]:
            # new station or location changed (just in case)
            need_update = True
            break

    # checking if there closed stations (not sure if it's even possible)
    if not need_update:
        for recorded_station in list(stations_in_records.keys()):
            if recorded_station not in stations_in_response:
                need_update = True
                break

    # rewriting list if something changed
    if need_update:
        with open(file, "w", encoding="utf-8", newline='') as csvfile:
            writer = csv.writer(csvfile, quotechar='"')
            for station in response:
                writer.writerow([station['Id'], station['Position'], station['Address'],
                                station['TotalOrdinaryPlaces'], station['TotalElectricPlaces']])

    # print(need_update)


def prepare_file_for_deltas(response, file):
    create_new = False  # flag, true if file is new and nothing recorded (or just 1 element in row)

    try:
        with open(file, "r", encoding="utf-8", newline='') as csvfile:
            reader = csv.reader(csvfile, quotechar='"')
            if len(list(reader)) == 0:  # empty file
                create_new = True
            for row in reader:
                if len(row) == 0 or len(row) == 1:  # useless string
                    create_new = True
    except FileNotFoundError:
        open(file, 'w')
        create_new = True

    if create_new:
        check_if_updates(response, 'stations.csv')

        with open(file, "w", encoding="utf-8", newline='') as csvfile:
            writer = csv.writer(csvfile, quotechar='"')

            writer.writerow(['Id', dt.datetime.now().strftime("%H:%M")])  # header

            for row in response:
                total_free_places_now = row["FreeElectricPlaces"] + row["FreeOrdinaryPlaces"]
                writer.writerow([row['Id'], total_free_places_now])


def record_deltas(response, file):
    tempfile = NamedTemporaryFile('w+t', encoding="utf-8", newline='', delete=False)
    prepare_file_for_deltas(response, file)

    unused_stations_in_response = stations_in_response.copy()

    with open(file, 'r', encoding="utf-8", newline='') as csvFile, tempfile:
        reader = csv.reader(csvFile, delimiter=',', quotechar='"')
        writer = csv.writer(tempfile, delimiter=',', quotechar='"')

        for row in reader:
            try:
                unused_stations_in_response.pop(unused_stations_in_response.index(row[0]))
                station_info = response[stations_in_response.index(row[0])]  # info for station by index of id
                total_free_places_now = station_info["FreeElectricPlaces"] + station_info["FreeOrdinaryPlaces"]
                if row[-1] != '-':
                    delta_info = f'{int(row[-1].split()[-1]) - total_free_places_now} {total_free_places_now}'
                else:
                    delta_info = f'{0} {total_free_places_now}'
                row.append(delta_info)
                writer.writerow(row)
            except ValueError:
                if str(row[0]) == 'Id':  # updating header
                    row.append(dt.datetime.now().strftime("%H:%M"))
                    writer.writerow(row)
                    number_of_records = len(row)  # number of records in file
                else:  # not mentioned in response station
                    row.append('-')
                    writer.writerow(row)

        for new_station in unused_stations_in_response:  # stations without previous records
            station_info = response[stations_in_response.index(new_station)]
            total_free_places_now = station_info["FreeElectricPlaces"] + station_info["FreeOrdinaryPlaces"]

            new_station_row = [station_info['Id']]
            new_station_row.extend(['-' for _ in range(number_of_records - 2)])
            new_station_row.append(f'0 {total_free_places_now}')

            # print(new_station_row)
            writer.writerow(new_station_row)

    shutil.move(tempfile.name, file)


check_if_updates(r['Items'], 'stations.csv')  # keeping list actual
record_deltas(r['Items'], 'deltas_test_emp.csv')  # adding deltas
