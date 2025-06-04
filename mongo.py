import os
import pymongo
from dotenv import load_dotenv
import pandas as pd
import warehouse_utils as warehouse
from supabase import create_client
import math


def main():
    load_dotenv()
    MONGO_URI = os.getenv("MONGODB_URI")
    client = pymongo.MongoClient(MONGO_URI)
    db = client["DATAFY"]
    historial = db["historial"]
    playlist = db["playlist"]

    print(playlist.find()) 

    historial_docs = list(historial.find())
    playlist_docs = list(playlist.find())

    df_historial = pd.DataFrame(historial_docs)
    df_historial.rename(columns={"_id": "id"}, inplace=True)
    df_historial = df_historial.astype({"id":str, "playlist_id":"Int64"})

    df_playlist = pd.DataFrame(playlist_docs)
    df_playlist.rename(columns={"_id": "id", "public": "publico"}, inplace=True)
    df_playlist_canciones = df_playlist[['id','canciones_id']]
    df_playlist = df_playlist.drop(['canciones_id'], axis=1)
    df_playlist = df_playlist.astype({"id":str})
    

    df_playlist_canciones = df_playlist_canciones.explode('canciones_id').reset_index(drop=True)

    df_playlist_canciones.rename(columns={"id": "playlist_id", "canciones_id": "cancion_id"}, inplace=True)
    df_playlist_canciones = df_playlist_canciones.astype({"playlist_id":str})


    json_historial = df_historial.to_dict(orient="records")
    json_playlist = df_playlist.to_dict(orient="records")
    json_play_canciones = df_playlist_canciones.to_dict(orient="records")
    
    url_warehouse = os.environ.get("SUPABASE_URL_WAREHOUSE")
    key_warehouse = os.environ.get("SUPABASE_KEY_WAREHOUSE")
    
    client = create_client(url_warehouse, key_warehouse)
    
    warehouse.load_table(client, "dim_playlist", json_playlist)
    warehouse.load_table(client, "hechos_reproducciones", json_historial)
    warehouse.load_table(client, "dim_playlist_canciones", json_play_canciones)

if __name__ == "__main__":
    main()