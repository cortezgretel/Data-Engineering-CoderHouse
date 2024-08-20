#Consigna de primer entregable.
#Gretel Penélope Cortés Macias

#Script que extraiga datos de una API pública y crear la tabla en Redshift para posterior carga de sus datos.
#El script debería extraer datos en JSON desde una API pública o gratuita para luego convertir estos datos en diccionario Python y posterior manipulación.

import requests
import base64
import psycopg2
from sqlalchemy import create_engine, text 
import json
from dotenv import load_dotenv # type: ignore
import os
import pandas as pd
import numpy as np

load_dotenv()  # Carga las variables del archivo .env

''' EXTRACCION'''


# 1. Obtener un token de acceso

def extract():
    dataframe = pd.DataFrame(columns=["pista_id", "pista_nombre", "artista_nombre", "album_nombre", "fecha_lanzamiento", "popularidad", "duracion" ])
    
    client_id = os.getenv("client_id")
    client_secret = os.getenv("client_secret")

    auth_url = 'https://accounts.spotify.com/api/token'
    auth_header = base64.b64encode((client_id + ':' + client_secret).encode('ascii')).decode('ascii')
    auth_data = {'grant_type': 'client_credentials'}

    auth_response = requests.post(auth_url, headers={'Authorization': 'Basic ' + auth_header}, data=auth_data)
    access_token = auth_response.json()['access_token']

    # 2. Extraer datos de la API de Spotify
    playlist_id = '37i9dQZEVXbMDoHDwVN2tF'  # Ejemplo: Top 50 México
    api_url = f'https://api.spotify.com/v1/playlists/{playlist_id}/tracks?offset=0&limit=100&market=MX'
    headers = {'Authorization': 'Bearer ' + access_token}
    response = requests.get(api_url, headers=headers)
    data = response.json()['items']

    for item in data:
        pista = item['track']
        pista_id = pista['id']
        pista_nombre = pista['name']
        artista_nombre = pista['artists'][0]['name']
        album_nombre = pista['album']['name']
        fecha_lanzamiento = pista['album']['release_date']
        popularidad = pista['popularity']
        duracion = pista['duration_ms']

        dataframe = pd.concat([dataframe, pd.DataFrame([{"pista_id": pista_id, 
                                             "pista_nombre": pista_nombre,
                                              "artista_nombre": artista_nombre,
                                               "album_nombre": album_nombre,
                                                "fecha_lanzamiento": fecha_lanzamiento,
                                                 "popularidad": popularidad,
                                                  "duracion": duracion}])], ignore_index=True)
        
    return dataframe


'''TRANSFORMACION'''

def transform(data):
    #convertir duracion a numerico
    data['duracion'] = pd.to_numeric(data['duracion'], errors='coerce')
    # Ajustar la columna 'duracion_ms'
    data['duracion'] = round(data['duracion'] / 10000, 2)
    
    # Convertir 'popularidad' a numérico, manejando errores
    data['popularidad'] = pd.to_numeric(data['popularidad'], errors='coerce')

    # Ajustar la columna 'popularidad'
    data['popularidad'] = round(data['popularidad'] / 10, 2)
    
    # Crear la columna 'popularidad_cadena' con base en los valores de 'popularidad'
    data['popularidad_cadena'] = np.where(data['popularidad'] > 9, 'popular',
                                 np.where(data['popularidad'] > 6, 'media popular',
                                 'no es tan popular'))
    
    return data


'''Carga '''
def load(data):
    # Obtener las credenciales de conexión desde las variables de entorno
    dbname = os.getenv("dbname")
    user = os.getenv("user")
    password = os.getenv("password")
    host = os.getenv("host")
    port = os.getenv("port")

    # Crear la cadena de conexión
    url = f'postgresql://{user}:{password}@{host}:{port}/{dbname}'

    # Conectar a la base de datos usando SQLAlchemy
    engine = create_engine(url)


    with engine.connect() as conn:
        # Eliminar la tabla si ya existe
        conn.execute(text('DROP TABLE IF EXISTS gretel141000_coderhouse."pistasSpotify"'))

        # Ahora carga los datos
        try:
            data.to_sql("pistasSpotify", engine, schema="gretel141000_coderhouse", index=False, if_exists='replace')
            print("Datos cargados exitosamente en Redshift.")
        except Exception as e:
            print(f"Error al cargar los datos en Redshift: {e}")


extracted_data = extract() 
transformed_data = transform(extracted_data) 
print("Transformed Data") 
print(transformed_data) 
load(transformed_data) 


    



