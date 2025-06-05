import os
import pandas as pd
from supabase import create_client
import pymongo
import database_utils as database
import warehouse_utils as warehouse
import mongo_utils as mongo
from exceptions import NoNewDataException

# TABLE_NAMES = {
#     "Artistas":"dim_artistas",
#     "Albumes":"dim_album",
#     "Canciones":"dim_canciones",
#     "Colaboraciones":"dim_colaboraciones",
#     "Genero":"dim_genero",
#     "Paises":"dim_paises",
#     "Plan":"dim_plan",
#     "Usuarios":"dim_usuarios"
# }

def process_canciones(old_data: list[dict], new_data: list[dict]) -> tuple[list[dict], list[int]]:
    old_df = pd.DataFrame(data=old_data)
    new_df = pd.DataFrame(data=new_data)

    old_df["key"] = old_df["cancion_id"].astype(str) + "-" + old_df["playlist_id"].astype(str)
    new_df["key"] = new_df["cancion_id"].astype(str) + "-" + new_df ["playlist_id"].astype(str)

    df_deleted = old_df[~old_df["key"].isin(new_df["key"])]
    df_added = new_df[~new_df["key"].isin(old_df["key"])]

    df_added = df_added.drop(columns=["key"])

    deleted = df_deleted["id"].to_list()
    added = df_added.to_dict('records')

    return added, deleted


# Este orden es muy importante
TABLE_NAMES_SQL = {
    "dim_plan":database.get_new_plan,
    "dim_genero":database.get_new_genero,
    "dim_paises":database.get_new_paises,
    "dim_artistas":database.get_new_artistas, # Sacar para hacer upsert
    "dim_album":database.get_new_albumes,
    "dim_canciones":database.get_new_cancion,
    "dim_colaboraciones":database.get_new_colaboraciones,
    "dim_usuarios":database.get_new_users, # Sacar para hacer upsert
}

def main():
    url_source = os.environ.get("SUPABASE_URL_SOURCE")
    key_source = os.environ.get("SUPABASE_KEY_SOURCE")
    url_warehouse = os.environ.get("SUPABASE_URL_WAREHOUSE")
    key_warehouse = os.environ.get("SUPABASE_KEY_WAREHOUSE")
    
    client_db = create_client(url_source, key_source)
    client_wh = create_client(url_warehouse, key_warehouse)
    
    for wh_table_name, func in TABLE_NAMES_SQL.items():
        try:
            last_id = warehouse.last_id(client_wh, wh_table_name)
            new_data = func(client_db, last_id)
            warehouse.load_table(client_wh, wh_table_name, new_data)
            print("Updated: " + wh_table_name)
        except NoNewDataException as e:
            print(e)
    
    
    mongo_uri = os.environ.get("MONGODB_URI")
    client = pymongo.MongoClient(mongo_uri)
    db = client["DATAFY"]
    
    try:
        last_id = warehouse.last_id(client_wh, "hechos_reproducciones")
        new_reproduccion_data = mongo.get_new_historial(db, last_id)
        warehouse.load_table(client_wh, "hechos_reproducciones", new_reproduccion_data)
        print("Updated: hechos_reproducciones")
    except NoNewDataException as e:
        print(e)

    try:
        playlist_data, play_cancion_data = mongo.get_playlist(db)
        warehouse.load_table(client_wh, "dim_playlist", playlist_data)
        print("Updated: dim_playlist")
        
        old_play_cancion_data = warehouse.get_all_playlist_songs(client_wh)
        added, deleted = process_canciones(old_play_cancion_data, play_cancion_data)
        warehouse.load_table(client_wh, "dim_playlist_canciones", added)
        warehouse.delete_from_table(client_wh, "dim_playlist_canciones", deleted)

        print("Updated: dim_playlist_canciones")
    except NoNewDataException as e:
        print(e)


if __name__ == "__main__":
    main()