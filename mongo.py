import os
import pymongo
from dotenv import load_dotenv

load_dotenv()
MONGO_URI = os.getenv("MONGODB_URI")

client = pymongo.MongoClient(MONGO_URI)
db = client["test"]
users = db["users"]
users.insert_one({"name": "tomas"})