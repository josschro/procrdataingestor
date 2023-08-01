import datetime
import logging
import logging
import pandas as pd
import requests
import json
import numpy as np
import sys
from sys import path
import os
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
import datetime

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, dir_path)

import azure.functions as func
from cr_utils import connect, get_sensors, insert_sensordata,  insert_data


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    # Database settings
    param_dic = {
    "host": "psqlpro14.postgres.database.azure.com",
    "database": "db",
    "user": "db",
    "password": "pw"
    }

    KVUri = f"https://proappkv.vault.azure.net"
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=KVUri, credential=credential)
    logging.info("Getting client secrets")
    secretName = client.get_secret("psqlpro-user").value
    secretName = secretName
    secretPassword = client.get_secret("psqlpro-password").value
    secretApikey = client.get_secret("cr-apikey-test").value

    param_dic["password"] = secretPassword
    param_dic["database"] = "testdb"
    param_dic["user"] = secretName
    headers = {
        "accept" : "application/json",
        "X-Api-Key": secretApikey
    }

    logging.info("Getting sensors from api:")
    response = requests.get('https://cr-api-prod.azurewebsites.net/sensors/', headers=headers)
    text1 = response.text
    sensor_api = json.loads(text1)

    number_of_sensors = len(sensor_api)
    df_sensor_api = pd.DataFrame(sensor_api)
    logging.info(df_sensor_api)
    sensor_list_api = []
    i = 0
    while i < number_of_sensors:
        sensor = sensor_api[i]['id']
        sensor_list_api.append(sensor)
        i += 1

    logging.info("Getting sensors from database:")
    logging.info("Making connection to database")
    conn = connect(param_dic)

    # Check if sensorlist in database is complete 
    sensors_db = get_sensors(conn)
    sensor_list_db = sensors_db['sensor_id'].tolist()
    missing_list = np.setdiff1d(sensor_list_api,sensor_list_db).tolist()

    if len(missing_list):

        missing_sensors = df_sensor_api.loc[df_sensor_api['id'].isin(missing_list)]
        values = missing_sensors.to_numpy()
        tuples = tuple(map(tuple, values))
        insert_sensordata(conn, tuples)

    else:
        logging.info("All sensors present in database")
    conn.close()

    for index, row in df_sensor_api.iterrows():
        conn = connect(param_dic)
        id = row['id']
        logging.info("Getting data data from api:")
        response = requests.get(f'https://cr-api-prod.azurewebsites.net/sensors/{id}', headers=headers)
        text2 = response.text
        data_api = json.loads(text2)
        if type(data_api) == list:
            ins_data = data_api[-1] # Assuming the last item is the most recent measurement, if not: change to [0] for first item
        else:
            ins_data = data_api
        insert_data(conn, ins_data)
        conn.close()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
