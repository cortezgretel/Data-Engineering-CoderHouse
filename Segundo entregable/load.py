from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

def load(data):
    dbname = os.getenv("dbname")
    user = os.getenv("user")
    password = os.getenv("password")
    host = os.getenv("host")
    port = os.getenv("port")

    url = f'postgresql://{user}:{password}@{host}:{port}/{dbname}'
    engine = create_engine(url)

    with engine.connect() as conn:
        conn.execute(text('DROP TABLE IF EXISTS gretel141000_coderhouse."pistasSpotify"'))
        try:
            data.to_sql("pistasSpotify", engine, schema="gretel141000_coderhouse", index=False, if_exists='replace')
            print("Datos cargados exitosamente en Redshift.")
        except Exception as e:
            print(f"Error al cargar los datos en Redshift: {e}")
