import os
from supabase import create_client
import database_utils as database
import warehouse_utils as warehouse

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
TABLE_NAMES = {
    "dim_plan":database.get_new_plan,
    "dim_genero":database.get_new_genero,
    "dim_paises":database.get_new_paises,
    "dim_artistas":database.get_new_artistas,
    "dim_album":database.get_new_albumes,
    "dim_canciones":database.get_new_cancion,
    "dim_colaboraciones":database.get_new_colaboraciones,
    "dim_usuarios":database.get_new_users,
}

def main():
    url_source = os.environ.get("SUPABASE_URL_SOURCE")
    key_source = os.environ.get("SUPABASE_KEY_SOURCE")
    url_warehouse = os.environ.get("SUPABASE_URL_WAREHOUSE")
    key_warehouse = os.environ.get("SUPABASE_KEY_WAREHOUSE")
    
    client_db = create_client(url_source, key_source)
    client_wh = create_client(url_warehouse, key_warehouse)
    
    for wh_table_name, func in TABLE_NAMES.items():
        last_id = warehouse.last_id(client_wh, wh_table_name)
        new_data = func(client_db, last_id)
        warehouse.load_table(client_wh, wh_table_name, new_data)
        print("Updated: " + wh_table_name)


if __name__ == "__main__":
    main()