import os
from supabase import create_client
import database_utils as database
import warehouse_utils as warehouse


def main():
    url_source = os.environ.get("SUPABASE_URL_SOURCE")
    key_source = os.environ.get("SUPABASE_KEY_SOURCE")
    url_warehouse = os.environ.get("SUPABASE_URL_WAREHOUSE")
    key_warehouse = os.environ.get("SUPABASE_KEY_WAREHOUSE")
    
    client_database = create_client(url_source, key_source)
    client_warehouse = create_client(url_warehouse, key_warehouse)
    
    last_id = warehouse.last_id(client_warehouse, "dim_plan")
    # new_data = database.get_new_entries(client_database, "Usuarios", last_id)
    # warehouse.load_table(client_warehouse, "dim_usuarios", new_data)
    
    


if __name__ == "__main__":
    main()