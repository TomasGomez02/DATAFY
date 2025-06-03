import os
from supabase import create_client, Client

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

cliente: Client = create_client(url, key)

response = (
    cliente.table("Paises")
    .select("id", "nombre")
    .eq("id", 89)
    .execute()
)

print(response)