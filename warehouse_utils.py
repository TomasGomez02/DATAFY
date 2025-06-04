from supabase import Client

def load_users(client: Client, data: list[dict]):
    client.table("dim_usuarios").upsert(data).execute()

def load_artists(client: Client, data: list[dict]):
    client.table("dim_artistas").upsert(data).execute()

def load_table(client: Client, table_name: str, data: list[dict]):
    client.table(table_name).upsert(data).execute()

