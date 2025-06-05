import os
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

# Este orden es muy importante

TABLE_NAMES_SQL_1 = {
    "dim_plan":database.get_new_plan,
    "dim_genero":database.get_new_genero,
    "dim_paises":database.get_new_paises
}

TABLE_NAMES_SQL_2 = {
    "dim_album":database.get_new_albumes,
    "dim_canciones":database.get_new_cancion,
    "dim_colaboraciones":database.get_new_colaboraciones,
}

def main():
    url_source = os.environ.get("SUPABASE_URL_SOURCE")
    key_source = os.environ.get("SUPABASE_KEY_SOURCE")
    url_warehouse = os.environ.get("SUPABASE_URL_WAREHOUSE")
    key_warehouse = os.environ.get("SUPABASE_KEY_WAREHOUSE")
    
    client_db = create_client(url_source, key_source)
    client_wh = create_client(url_warehouse, key_warehouse)
    
    for wh_table_name, func in TABLE_NAMES_SQL_1.items():
        try:
            last_id = warehouse.last_id(client_wh, wh_table_name)
            new_data = func(client_db, last_id)
            warehouse.load_table(client_wh, wh_table_name, new_data)
            print("Updated: " + wh_table_name)
        except NoNewDataException as e:
            print(e)
    
    for new_data in database.get_all_table(client_db, "Artistas"):
        tf = database.transform(new_data, drop=["password_hash"], rename={"pais":"id_pais"})
        
        warehouse.load_table(client_wh, "dim_artistas", tf)
        print("Updated: " + "dim_artistas")
    
    for wh_table_name, func in TABLE_NAMES_SQL_2.items():
        try:
            last_id = warehouse.last_id(client_wh, wh_table_name)
            new_data = func(client_db, last_id)
            warehouse.load_table(client_wh, wh_table_name, new_data)
            print("Updated: " + wh_table_name)
        except NoNewDataException as e:
            print(e)
    
    for new_data in database.get_all_table(client_db, "Usuarios"):
        tf = database.transform(new_data, ["password_hash"], {"plan_id":"id_plan", "pais_id":"id_pais"})
        warehouse.load_table(client_wh, "dim_usuarios", tf)
        print("Updated: " + "dim_usuarios")
    
    
    mongo_uri = os.environ.get("MONGODB_URI")
    client = pymongo.MongoClient(mongo_uri)
    db = client["DATAFY"]
    
    last_id = warehouse.last_id(client_wh, "dim_playlist")
    try:
        new_playlist_data, new_play_cancion_data = mongo.get_new_playlist(db, last_id)
        warehouse.load_table(client_wh, "dim_playlist", new_playlist_data)
        print("Updated: dim_playlist")
        
        warehouse.load_table(client_wh, "dim_playlist_canciones", new_play_cancion_data)
        print("Updated: dim_playlist_canciones")
    except NoNewDataException as e:
        print(e)
    
    try:
        last_id = warehouse.last_id(client_wh, "hechos_reproducciones")
        new_reproduccion_data = mongo.get_new_historial(db, last_id)
        warehouse.load_table(client_wh, "hechos_reproducciones", new_reproduccion_data)
        print("Updated: hechos_reproducciones")
    except NoNewDataException as e:
        print(e)



if __name__ == "__main__":
    main()