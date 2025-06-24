import os
import json
from pymongo import MongoClient
from dotenv import load_dotenv
load_dotenv()

if __name__ == "__main__":
    json_folder = "./json_output" 
    MONGO_URI = os.getenv("MONGO_URI")
    client = MongoClient(MONGO_URI)
    db = client["nepal_location"]

    json_files = [f for f in os.listdir(json_folder) if f.endswith(".json")]

    for json_file in json_files:
        json_path = os.path.join(json_folder, json_file)
        with open(json_path, "r", encoding="utf-8") as f:
            records = json.load(f)

        collection_name = json_file.replace(".json", "")

        if records:
            db[collection_name].insert_many(records)
            print(f" Inserted {len(records)} records into MongoDB collection: {collection_name}")
        else:
            print(f" No records to insert for collection: {collection_name}")
