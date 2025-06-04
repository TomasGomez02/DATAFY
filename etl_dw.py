import os
from supabase import create_client
import database_utils as database
import warehouse_utils as warehouse

from pprint import pprint
import pandas as pd

def main():
    url_source = os.environ.get("SUPABASE_URL_SOURCE")
    key_source = os.environ.get("SUPABASE_KEY_SOURCE")
    url_warehouse = os.environ.get("SUPABASE_URL_WAREHOUSE")
    key_warehouse = os.environ.get("SUPABASE_KEY_WAREHOUSE")
    
    client_database = create_client(url_source, key_source)
    client_warehouse = create_client(url_warehouse, key_warehouse)
    
    response = database.get_plan_table(client_database)
    warehouse.load_table(client_warehouse, "dim_plan", response)
    
    


if __name__ == "__main__":
    main()