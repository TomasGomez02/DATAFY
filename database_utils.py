from supabase import Client
import pandas as pd

def get_table_from_date(client: Client, table_name: str, date_column_name: str, date: str) -> list[dict]:
    return (client
                .table(table_name)
                .select("*")
                .gte(date_column_name, date)
                .execute()
                .model_dump()["data"])

def get_table(client: Client, table_name: str) -> list[dict]:
    return client.table(table_name).select("*").execute().model_dump()["data"]

def get_users_from_date(client: Client, date: str) -> list[dict]:
    "La fecha debe estar en formato yyyy-mm-dd"
    response = get_table_from_date(client, "Usuarios", "fecha_creacion", date)
    df = pd.DataFrame(response)
    transformed = df.rename(columns={"plan_id":"id_plan"})
    return transformed.to_dict(orient="records")

def get_artists_from_date(client: Client, date: str) -> list[dict]:
    response = get_table_from_date(client, "Artistas", "fecha_creacion", date)
    df = pd.DataFrame(response)
    transformed = df.rename(columns={"pais":"id_pais"})
    return transformed.to_dict(orient="records")

def get_plan_table(client: Client):
    response = get_table(client, "Plan")
    df = pd.DataFrame(response)
    transformed = df.rename(columns={"nombre":"nombre_plan"})
    return transformed.to_dict(orient="records")

def get_album_from_date(client: Client, date: str):
    response = get_table_from_date(client, "Album", "fecha_lanzamiento", date)
    df = pd.DataFrame(response)
    transformed = df.rename(columns={"artista_id":"id_artista"})
    return transformed.to_dict(orient="records")

def get_cancion_from_date(client: Client, date: str):
    albums = get_table_from_date(client, "Album", "fecha_lanzamiento", date)
    """
    SELECT c.id, c.titulo, c...
    FROM Canciones as c
    JOIN Album as a ON c.album_id = a.id
    WHERE a.fecha_lanzamiento >= {date}
    """
    # Hay que hacer un join para solo actualizar las canciones 
    # que pertenezcan a un album a partir de cierta fecha
    pass

def get_genero(client: Client):
    return get_table(client, "Genero")

def get_paises(client: Client):
    return get_table(client, "Paises")

def get_colaboraciones_from_date(client: Client):
    # Hay que hacer un join para solo actualizar las colaboraciones a partir de cierta fecha
    pass