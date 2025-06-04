import os
import pymongo
from dotenv import load_dotenv
import pandas as pd

load_dotenv()
MONGO_URI = os.getenv("MONGODB_URI")


client = pymongo.MongoClient(MONGO_URI)
db = client["DATAFY"]
historial = db["historial"]
playlist = db["playlist"]


print(historial.find()) 
print(playlist.find()) 

historial_docs = list(historial.find())
playlist_docs = list(playlist.find())

df_historial = pd.DataFrame(historial_docs)
df_historial.rename(columns={"_id": "id"}, inplace=True)


df_playlist = pd.DataFrame(playlist_docs)
df_playlist.rename(columns={"_id": "id", "public": "publico"}, inplace=True)
df_playlist_canciones = df_playlist[['id','canciones_id']]
df_playlist = df_playlist.drop(['canciones_id'], axis=1)

df_playlist_canciones = df_playlist_canciones.explode('canciones_id').reset_index(drop=True)

df_playlist_canciones.rename(columns={"id": "playlist_id", "canciones_id": "cancion_id"}, inplace=True)

print(df_historial)
print(df_playlist)
print(df_playlist_canciones)