from supabase import Client

def load_table(client: Client, table_name: str, data: list[dict]):
    client.table(table_name).upsert(data).execute()

def last_id(client: Client, table_name: str) -> int:
    response = (client
            .table(table_name)
            .select("id")
            .order("id", desc=True)
            .limit(1)
            .execute()
            .model_dump()["data"])
    return response[0]["id"] if len(response) > 0 else 0