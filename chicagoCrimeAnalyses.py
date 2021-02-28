import pandas as pd
import numpy as np
from sodapy import Socrata
import datetime, sqlite3, math
from os import path

def dataframe(datalink):
    client = Socrata(datalink, None)
    # first 100 results in json
    results = client.get("x2n5-8w5q", limit = 1000000)
    results_df = pd.DataFrame.from_records(results)

    time_stamps = []
    day_of_week = []

    for entry in results_df['date_of_occurrence']:
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

    for entry in results_df['latitude']:
        latRadians = float(entry) * math.pi / 180
        lat_radians.append(latRadians)
    
    for entry in results_df['longitude']:
        lonRadians = float(entry) * math.pi / 180
        lon_radians.append(lonRadians)
    
    results_df['time'] = time_stamps
    results_df['day'] = day_of_week
    results_df['lat_radians'] = lat_radians
    results_df['lon_randians'] = lonRadians

    results_df = results_df.drop(['date_of_occurrence','case_', '_iucr', 'fbi_cd', 'x_coordinate',
                    'y_coordinate', ':@computed_region_43wa_7qmu',
                    ':@computed_region_bdys_3d7i', ':@computed_region_vrxf_vc4k', ':@computed_region_6mkv_f3dw',
                    ':@computed_region_rpca_8um6', ':@computed_region_awaf_s7ux', 'location', 'beat', 'ward'], axis = 1)
    # ':@computed_region_6mkv_f3dw' => zip code
    # ':@computed_region_rpca_8um6' => zip code boundaries
    # ^ garbage api, these are not 'zipcodes' and there is no documentation for what these numbers mean. ~press shame button~
    return results_df

def database():
    if path.exists('chicago_crime.db') == False:
        conn = sqlite3.connect('chicago_crime.db')
        dataframe("data.cityofchicago.org").to_sql('chicago_crime_table', conn, if_exists = 'replace')
        sqlQuery()
    else:
        sqlQuery()

def sqlQuery():
    conn = sqlite3.connect('chicago_crime.db')
    cur = conn.cursor()

    # user_destination_block = input("Enter destination block: ")
    # user_destination_time = input("Enter time that you will arrive: ")
    user_lat = input("Enter lat: ")
    user_lon = input("Enter lon: ")
    user_lat_rad = float(user_lat) * math.pi / 180
    user_lon_rad = float(user_lon) * math.pi / 180

    # sqlite does not support trig
    sql_location = "SELECT * FROM chicago_crime_table WHERE acos(sin({}) * sin(Lat) + cos({}) * cos(Lat) * cos(Lon - ({}))) * 6371 <= 1000".format(user_lat_rad, user_lat_rad, user_lon_rad)

    cur.execute(sql_location)
    relevant = cur.fetchall()
    for x in block_result:
        print(x)
    
def main():
    api_link = "data.cityofchicago.org"
    # print(dataframe(api_link))
    database()
main()