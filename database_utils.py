from supabase import Client
import pandas as pd

def get_new_entries(client: Client, table_name: str, last_id: int) -> list[dict]:
    response = (client
                .table(table_name)
                .select("*")
                .gte("id", last_id)
                .execute()
                .model_dump()["data"])
    
    return response

def transform(data: list[dict], drop: list[str]=None, rename: dict[str, str]=None) -> list[dict]:
    df = pd.DataFrame(data)
    if drop:
        df = df.drop(drop, axis=1)
    if rename:
        df = df.rename(columns=rename)
    return df.to_dict(orient="records")

def get_new_users(clinet: Client, last_id: int) -> list[dict]:
    data = get_new_entries(clinet, "Usuarios", last_id)
    tf = transform(data, ["password_hash"], {"plan_id":"id_plan", "pais_id":"id_pais"})
    return tf

def get_new_artistas(client: Client, last_id: int) -> list[dict]:
    data = get_new_entries(client, "Artistas", last_id)
    tf = transform(data, drop=["password_hash"], rename={"pais":"id_pais"})
    return tf

def get_new_plan(client: Client, last_id: int) -> list[dict]:
    data = get_new_entries(client, "Plan", last_id)
    tf = transform(data, rename={"nombre":"nombre_plan"})
    return tf

def get_new_albumes(client: Client, last_id: int) -> list[dict]:
    data = get_new_entries(client, "Albumes", last_id)
    tf = transform(data, rename={"artista_id":"id_artista"})
    return tf

def get_new_cancion(client: Client, last_id: int) -> list[dict]:
    data = get_new_entries(client, "Canciones", last_id)
    tf = transform(data, rename={"genero_id":"id_genero", "artista_principal_id":"id_artista_principal",
                                 "album_id":"id_album"})
    return tf

def get_new_genero(client: Client, last_id: int) -> list[dict]:
    data = get_new_entries(client, "Genero", last_id)
    tf = transform(data)
    return tf

def get_new_paises(client: Client, last_id: int) -> list[dict]:
    data = get_new_entries(client, "Paises", last_id)
    tf = transform(data)
    return tf

def get_new_colaboraciones(client: Client, last_id: int) -> list[dict]:
    data = get_new_entries(client, "Colaboraciones", last_id)
    tf = transform(data, rename={"cancion_id":"id_cancion", "artista_id":"id_artista"})
    return tf