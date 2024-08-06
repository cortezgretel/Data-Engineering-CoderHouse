#Consigna de primer entregable.
#Gretel Penélope Cortés Macias

#Script que extraiga datos de una API pública y crear la tabla en Redshift para posterior carga de sus datos.
#El script debería extraer datos en JSON desde una API pública o gratuita para luego convertir estos datos en diccionario Python y posterior manipulación.

import requests
import base64
import psycopg2
import json

# 1. Obtener un token de acceso
client_id = 'e81b4379e75f4d7fb4e7cc667ff53f44'
client_secret = '07ae04b03f234242846c78cc099aebe1'

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

# 3. Conectar a Redshift
conn = psycopg2.connect(
    dbname="data-engineer-database",
    user="gretel141000_coderhouse",
    password="1qn0fgE80l",
    host="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com",
    port="5439"
)
cursor = conn.cursor()

# 4. Crear una tabla en Redshift
create_table_query = """
CREATE TABLE IF NOT EXISTS gretel141000_coderhouse.pistasSpotify (
    pista_id VARCHAR(255) PRIMARY KEY,
    pista_nombre VARCHAR(255),
    artista_nombre VARCHAR(255),
    album_nombre VARCHAR(255),
    fecha_lanzamiento DATE,
    popularidad INT,
    duracion_ms INT
);
"""

cursor.execute(create_table_query)

conn.commit()

# 5. Cargar los datos en Redshift
for item in data:
    pista = item['track']
    pista_id = pista['id']
    pista_nombre = pista['name']
    artista_nombre = pista['artists'][0]['name']
    album_nombre = pista['album']['name']
    fecha_lanzamiento = pista['album']['release_date']
    popularidad = pista['popularity']
    duracion_ms = pista['duration_ms']



    try:
        # Intentar insertar el registro
        insert_query = """
        INSERT INTO gretel141000_coderhouse.pistasSpotify (pista_id, pista_nombre, artista_nombre, album_nombre, fecha_lanzamiento, popularidad, duracion_ms)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (pista_id, pista_nombre, artista_nombre, album_nombre, fecha_lanzamiento, popularidad, duracion_ms))

    except psycopg2.IntegrityError:
        # Si ocurre un conflicto, actualizar el registro existente
        conn.rollback()
        update_query = """
        UPDATE gretel141000.pistasSpotify SET
        pista_nombre = EXCLUDED.pista_nombre,
        artista_nombre = EXCLUDED.artista_nombre,
        album_nombre = EXCLUDED.album_nombre,
        fecha_lanzamiento = EXCLUDED.fecha_lanzamiento,
        popularidad = EXCLUDED.popularidad,
        duracion_ms = EXCLUDED.duracion_ms
        WHERE pista_id = %s;
        """
        
        cursor.execute(insert_query, (pista_nombre, artista_nombre, album_nombre, fecha_lanzamiento, popularidad, duracion_ms, pista_id))
    conn.commit()

# Cerrar la conexión
cursor.close()
conn.close()




