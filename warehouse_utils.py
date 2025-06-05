from supabase import Client

def load_table(client: Client, table_name: str, data: list[dict]):
    client.table(table_name).upsert(data).execute()

def delete_from_table(client: Client, table_name: str, ids: list[int]):
    client.table(table_name).delete().in_("id", ids).execute()

def get_all_playlist_songs(client: Client) -> list[dict]:
    end_id = (client
              .table("dim_playlist_canciones")
              .select("id")
              .order("id", desc=True)
              .limit(1)
              .execute()
              .model_dump()["data"])
    end_id = end_id[0]["id"] if len(end_id) > 0 else 0

    n = 1
    playlist_songs = []
    while n < end_id:
        response: list[dict] = (client
                                .table("dim_playlist_canciones")
                                .select("*")
                                .range(n, n+1000)
                                .execute()
                                .model_dump()["data"])
        playlist_songs += response
        n += 1000
    return playlist_songs

def last_id(client: Client, table_name: str) -> int:
    response = (client
            .table(table_name)
            .select("id")
            .order("id", desc=True)
            .limit(1)
            .execute()
            .model_dump()["data"])
    return response[0]["id"] if len(response) > 0 else 0