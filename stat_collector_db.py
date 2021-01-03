# -*- coding: utf-8 -*-
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from mysql.connector import MySQLConnection, Error
from tempfile import NamedTemporaryFile
from configparser import ConfigParser
import datetime as dt
import requests
import warnings
import shutil
import json
import csv
import os

# get response
warnings.simplefilter('ignore', InsecureRequestWarning)
r = requests.get('http://194.58.122.45:6536/ajax/parkings/', verify=False).text
r = r.replace('false', '0')
r = r.replace('true', '1')
r = json.loads(r)

# all stations mentioned in response
stations_in_response = []
for station in r['Items']:
    stations_in_response.append(station['Id'])

# logging preparations (26 - 43)
log_filename = "stat_collector.log"

if not os.path.exists(log_filename):
    open(log_filename, 'w')


def write_log(text):
    timestamp = dt.datetime.strftime(dt.datetime.now(), "%Y-%m-%d %H:%M:%S")
    with open(log_filename, 'a') as f:
        f.write("{} {}".format(timestamp, text))
        f.write("\n")


def read_db_config(filename='config.ini', section='mysql'):
    # create parser and read ini configuration file
    parser = ConfigParser()
    parser.read(filename)

    # get section, default to mysql
    db = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            db[item[0]] = item[1]
    else:
        raise Exception('{0} not found in the {1} file'.format(section, filename))

    return db


def prepare_file_for_deltas(response, file):  # utility file
    create_new = False  # flag, true if file is new and nothing recorded

    try:  # inspecting
        with open(file, "r", encoding="utf-8", newline='') as csvfile:
            reader = csv.reader(csvfile, quotechar='"')
            if len(list(reader)) == 0:  # empty file
                create_new = True
            for row in reader:  # row in utility file
                if len(row) == 0 or len(row) == 1:  # confused string
                    create_new = True
    except FileNotFoundError:
        create_new = True

    if create_new:  # creating if needed
        with open(file, "w", encoding="utf-8", newline='') as csvfile:
            writer = csv.writer(csvfile, quotechar='"')

            writer.writerow(['Id', dt.datetime.now().strftime("%Y-%m-%d %H:%M:00")])  # header

            for row in response:
                writer.writerow([row['Id'], row["FreeOrdinaryPlaces"], row["FreeElectricPlaces"]])


def record_deltas(response, file):
    tempfile = NamedTemporaryFile('w+t', encoding="utf-8", newline='', delete=False)

    current_time = dt.datetime.now().strftime("%Y-%m-%d %H:%M:00")

    db_config = read_db_config()
    try:
        conn = MySQLConnection(**db_config)
        if not conn.is_connected():
            print('connection failed')
            return 0
        cursor = conn.cursor()
    except Error as error:
        print(error)
        write_log(error)
        return 0

    unused_stations_in_response = stations_in_response.copy()

    with open(file, 'r', encoding="utf-8", newline='') as csvFile, tempfile:
        reader = csv.reader(csvFile, delimiter=',', quotechar='"')
        writer = csv.writer(tempfile, delimiter=',', quotechar='"')

        for row in reader:  # row in utility file
            try:
                unused_stations_in_response.pop(unused_stations_in_response.index(row[0]))
                station_info = response[stations_in_response.index(row[0])]  # info for station by index of velobike_id

                cursor.execute('INSERT INTO collected_data '
                               '(velobike_id, timestamp, ordinarybikes_available, electrobikes_available) '
                               'VALUES (%s, %s, %s, %s)',
                               (row[0], current_time,
                                station_info["AvailableOrdinaryBikes"], station_info["AvailableElectricBikes"]))

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
                                   'VALUES (%s, %s, NULL, NULL)',
                                   (row[0], current_time))

                writer.writerow(row)

        for new_station in unused_stations_in_response:  # stations without previous records in utility file
            station_info = response[stations_in_response.index(new_station)]

            cursor.execute('INSERT INTO collected_data '
                           '(velobike_id, timestamp, ordinarybikes_available, electrobikes_available) '
                           'VALUES (%s, %s, %s, %s)',
                           (row[0], current_time,
                            station_info["AvailableOrdinaryBikes"], station_info["AvailableElectricBikes"]))

            new_station_row = [station_info['Id'],
                               station_info["FreeOrdinaryPlaces"], station_info["FreeElectricPlaces"]]
            writer.writerow(new_station_row)

    shutil.move(tempfile.name, file)

    conn.commit()
    cursor.close()
    conn.close()


prepare_file_for_deltas(r['Items'], 'temp_records.csv')  # creating utility file on start if missing

try:
    record_deltas(r['Items'], 'temp_records.csv')  # adding deltas
except:
    write_log('records went wrong')
