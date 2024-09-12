import requests
import base64
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def extract():
    dataframe = pd.DataFrame(columns=["pista_id", "pista_nombre", "artista_nombre", "album_nombre", "fecha_lanzamiento", "popularidad", "duracion" ])

    client_id = os.getenv("client_id")
    client_secret = os.getenv("client_secret")

    auth_url = 'https://accounts.spotify.com/api/token'
    auth_header = base64.b64encode((client_id + ':' + client_secret).encode('ascii')).decode('ascii')
    auth_data = {'grant_type': 'client_credentials'}

    auth_response = requests.post(auth_url, headers={'Authorization': 'Basic ' + auth_header}, data=auth_data)
    access_token = auth_response.json()['access_token']

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
