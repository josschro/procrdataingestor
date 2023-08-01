import pandas as pd
import logging
from pathlib import Path
from datetime import timedelta
import sys
import psycopg2

def connect(param_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        logging.info('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**param_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn

def get_sensors(conn):
    select_query_sensors = f'select "id" from cr_sensors'
    cursor = conn.cursor()
    sensors = cursor.execute(select_query_sensors)
    sensors = cursor.fetchall()
    column_names = ["sensor_id"]
    df_sensors = pd.DataFrame(sensors, columns=column_names)
    df_sensors = df_sensors.reset_index()
    cursor.close()
    return df_sensors

def insert_sensordata(conn, tuples):
    cursor = conn.cursor()
    tuples = tuples
    insert_sensors_query = """INSERT INTO cr_sensors(id, slug, sensor_type, name) VALUES(%s, %s, %s, %s)"""
    for t in tuples:

        try:
            cursor.execute(insert_sensors_query, t)
            logging.info("New sensors added to database")
        except (Exception, psycopg2.DatabaseError) as error:
            logging.info("Error: %s" % error)
            cursor.close()
            return 1
        conn.commit()
    cursor.close()
    return

def insert_data(conn, data):
    cursor = conn.cursor()
    if data['sensor_type'] == "corrosion":
        columns = str(tuple(data.keys())[:-1] + tuple(data['interpretation'].keys())).replace("'", "")
        values = tuple(data.values())[:-1] + tuple(data['interpretation'].values())
        insert_query = f"insert into cr_corr_data{columns} values{values}"
        # logging.info(insert_query)
        try:
            cursor.execute(insert_query)
            logging.info("New corrosion data added to database")
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logging.info("Error: %s" % error)
            cursor.close()
            return 1


    elif data['sensor_type'] == "moisture":
        columns = tuple(data.keys())[:-2] + tuple(data['interpretation'].keys()) + tuple(data['risk'].keys())
        columns = str(columns).replace("'", "")
        values = tuple(data.values())[:-2] + tuple(data['interpretation'].values()) + tuple(data['risk'].values())
        insert_query = f"insert into cr_moist_data{columns} values{values}"
        # print(insert_query)
        try:
            cursor.execute(insert_query)
            logging.info("New moisture data added to database")
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logging.info("Error: %s" % error)
            cursor.close()
            return 1


    else:
        logging.info("sensor type does not exist!")

    cursor.close()
    return