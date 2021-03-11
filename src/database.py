import pandas as pd
import numpy as np
from os import path
from sodapy import Socrata
import sqlite3, datetime, math

# Create database and create DB (embedded for now)

def createDataframe(datalink):
  client = Socrata(datalink, None)
  api_entries = client.get("x2n5-8w5q", limit = 10000000)
  entries_dataframe = pd.DataFrame.from_records(api_entries)

  time_stamps = []
  day_of_week = []

  for entry in entries_dataframe['date_of_occurrence']:
    date = entry.split('T')[0]
    year, month, day = (int(x) for x in date.split('-'))
    answer = datetime.date(year, month, day)
    date1 = answer.strftime("%A")
    day_of_week.append(date1)
    time = entry.split('T')[1]
    time = time[0:5]
    time_stamps.append(time)

  lat_radians = []
  lon_radians = []

  for entry in entries_dataframe['latitude']:
    radians_entry = float(entry) * math.pi / 180
    lat_radians.append(radians_entry)

  for entry in entries_dataframe['longitude']:
    radians_entry = float(entry) * math.pi / 180
    lon_radians.append(radians_entry)

  entries_dataframe['time'] = time_stamps
  entries_dataframe['day'] = day_of_week
  entries_dataframe['lat_radians'] = lat_radians
  entries_dataframe['lon_radians'] = lon_radians

  return entries_dataframe

def createDatabase():
  dataframe = createDataframe('data.cityofchicago.org')
  if path.exists('chicago_crime.db') == False:
    conn = sqlite3.connect('chicago_crime.db')
    dataframe.to_sql('chicago_crime_table', conn, if exists = 'repl')
  else:
    pass


