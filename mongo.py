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


print(historial.find()) #['_id', 'user_id', 'song_id', 'timestamp', 'playlist_id']
print(playlist.find()) #['_id', 'user_id', 'song_id', 'timestamp', 'playlist_id']

historial_docs = list(historial.find())
playlist_docs = list(playlist.find())

df_historial = pd.DataFrame(historial_docs)
df_historial['id'] = range(len(df_historial))
df_historial = df_historial.drop(['_id'], axis=1)


df_playlist = pd.DataFrame(playlist_docs)
df_playlist_canciones = df_playlist[['_id','canciones_id']]
df_playlist = df_playlist.drop(['canciones_id'], axis=1)

df_playlist_canciones = df_playlist_canciones.explode('canciones_id').reset_index(drop=True)
df_playlist_canciones['id'] = range(len(df_playlist_canciones))

print(df_historial)
print(df_playlist)
print(df_playlist_canciones)



