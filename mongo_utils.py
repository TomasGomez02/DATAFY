import os
import pymongo
from dotenv import load_dotenv
import pandas as pd
import warehouse_utils as warehouse
from supabase import create_client
from pymongo.collection import Collection
from pymongo.database import Database
from pprint import pprint

def get_new_entries(db: Collection, last_id: int):
    response = db.find({"_id":{"$gt":last_id}})
    return response

def get_new_playlist(db: Database, last_id: int) -> tuple[list[dict], list[dict]]:
    cursor = get_new_entries(db["playlist"], last_id)
    df = pd.DataFrame(list(cursor))
    df.rename(columns={"_id": "id", "public": "publico", "song_id":"canciones_id"}, inplace=True)
    df_play_canciones = df[["id", "canciones_id"]]
    df = df.drop(["canciones_id"], axis=1)
    
    df_play_canciones = df_play_canciones.explode('canciones_id').reset_index(drop=True)
    df_play_canciones.rename(columns={"id": "playlist_id", "canciones_id": "cancion_id"}, inplace=True)
    return df.to_dict(orient="records"), df_play_canciones.to_dict(orient="records")

def get_new_historial(db: Database, last_id: int) -> list[dict]:
    cursor = get_new_entries(db["historial"], last_id)
    df = pd.DataFrame(list(cursor))
    
    df.rename(columns={"_id": "id"}, inplace=True)
    df = df.astype({"playlist_id":"Int64"})
    
    return df.to_dict(orient="records")


def main():
    load_dotenv()
    MONGO_URI = os.getenv("MONGODB_URI")
    client = pymongo.MongoClient(MONGO_URI)
    db = client["DATAFY"]

    url_warehouse = os.environ.get("SUPABASE_URL_WAREHOUSE")
    key_warehouse = os.environ.get("SUPABASE_KEY_WAREHOUSE")
    client = create_client(url_warehouse, key_warehouse)

    last_id = warehouse.last_id(client, "dim_playlist")
    playlist, play_canciones = get_new_historial(db, last_id)

if __name__ == "__main__":
    main()