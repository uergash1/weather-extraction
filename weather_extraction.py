# -*- coding: utf-8 -*-
"""
Created on Sun Jan 19 16:05:05 2020

@author: uerga
"""

import time
import sys
import csv
import requests
import sqlite3
from sqlite3 import Error
from os import path


database = r"data.db"

sql_create_weather_table = """ CREATE TABLE IF NOT EXISTS weather (
                                    city_id nvarchar(250) NOT NULL,
                                    time integer NOT NULL,
                                    summary text NOT NULL,
                                    windSpeed float NOT NULL,
                                    temperature float NOT NULL,
                                    uvIndex integer NOT NULL,
                                    visibility integer NOT NULL,
                                    PRIMARY KEY(city_id, time)
                                ); """
 
 
def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
 
    return conn
 
 
def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def insert_weather(weather_params):
    """
    Insert weather information into the weather table
    :param weather_params:
    :return: weather id
    """
    # create a database connection
    conn = create_connection(database)
    
    if conn is not None:   
        # create weather table
        create_table(conn, sql_create_weather_table)
        with conn:
            sql = ''' INSERT INTO weather(city_id,time,summary,windSpeed,temperature,uvIndex,visibility)
                      VALUES(?,?,?,?,?,?,?) '''
            cur = conn.cursor()
            cur.execute(sql, weather_params)
            return cur.lastrowid

def select_weather(query):
    """
    Selects rows from weather table
    :param conn: the Connection object
    :param query: SQL query
    :return:
    """
    # create a database connection
    conn = create_connection(database)
    
     # create tables
    if conn is not None:
        # create weather table
        create_table(conn, sql_create_weather_table)
        with conn:
            cur = conn.cursor()
            cur.execute(query)
         
            return cur.fetchall()

    
def valid_to_save(city_id):
    """
    Checks whether DB already contains weather information for a specific city within 1 minute range
    :param city id:
    :return: boolean
    """
    current_time = int(time.time())
    end = current_time - 59
    query = "SELECT * FROM weather WHERE city_id='{}' and time BETWEEN {} and {}".format(city_id, end, current_time)
    result = select_weather(query)
    if not result:
        return True
    else:
        return False
            

def darksky_request(lat, lon):
    darksky_api = "46062db30332f2db485c9015ba673659"
    url = 'https://api.darksky.net/forecast/'+darksky_api+'/'+str(lat)+','+str(lon)
    result = requests.get(url)
    return result.json()


def show_weather_for_10mins(city_id):
    """
    Shows weather information for a specified city: minimum temperature, maximum temperature, average temperature for the last 10 mins
    :param city id:
    """           
    current_time = int(time.time())
    end = current_time - 599
    query = "SELECT min(temperature) as min_temp, max(temperature) as max_temp, avg(temperature) as avg_temp FROM (SELECT temperature FROM weather WHERE city_id='{}' and time BETWEEN {} and {})".format(city_id, end, current_time)
    result = select_weather(query)
    if not result:
        print("No weather information for the last 10 minutes for " + str(city_id) + " in the local database")
    else:
        print("Below given weather information for the last 10 minutes for " + str(city_id) + " from the local database")
        print("Minimum temperature: " + str(result[0][0]))
        print("Maximum temperature: " + str(result[0][1]))
        print("Average temperature: " + str(result[0][2]))


def export_data_into_csv(path):
        query = "SELECT * FROM weather"
        result = select_weather(query)
        if not result:
            print("No weather information in DB")
        else:
            with open(path, 'w', newline='') as csvfile:
                spamwriter = csv.writer(csvfile, delimiter=',')
                spamwriter.writerow(['CITY_ID', 'TIME', 'SUMMARY', 'WIND SPEED', 'TEMPERATURE', 'UVINDEX', 'VISIBILITY'])
                for row in result:
                    spamwriter.writerow(row)
                            
def get_current_weather_information():
    """
    Gets the current weather information from http://api.darksky.net for all cities in cities.csv and saves them into the “weather” table in DB
    """
    
    # if cities.csv does not exist
    if not path.exists("cities.csv"): 
        sys.exit("File cities.csv does not exist in the current folder. Please create csv file with the data city_id, lat, lon.")
    
    with open('cities.csv', 'r') as file:
        reader = csv.reader(file, delimiter = ',')
        for row in reader:
            city_id = row[0]
            lat = row[1]
            lon = row[2]
            #checks if there is no weather information for the last a minute in DB 
            if valid_to_save(city_id):
                jsn_result = darksky_request(lat, lon)
                time = jsn_result["currently"]["time"]
                summary = jsn_result["currently"]["summary"]
                windSpeed = jsn_result["currently"]["windSpeed"]
                temperature = jsn_result["currently"]["temperature"]
                uvIndex = jsn_result["currently"]["uvIndex"]
                visibility = jsn_result["currently"]["visibility"]
                weather_params = (city_id, time, summary, windSpeed, temperature, uvIndex, visibility)
                insert_weather(weather_params)
    
def main():
    argv = sys.argv[1:]
    if not argv:
        get_current_weather_information()
    elif argv[0] == "city_id":
        try:
            show_weather_for_10mins(str(argv[1]))
        except IndexError:
            sys.exit("Please specify city_id if you want to run the program with city_id argument")
    elif argv[0] == "fname":
        try:
            export_data_into_csv(str(argv[1]))
        except IndexError:
            sys.exit("Please specify csv path if you want to run the program with fname argument")
    else:
        sys.exit("Usage: weather_extraction.py city_id [city_id] | fname [path_to_store_csv]")
        
        
if __name__ == '__main__':
    main()

