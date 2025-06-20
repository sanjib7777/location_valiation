from fastapi import FastAPI
from pydantic import BaseModel
from pymongo import MongoClient
import os
from dotenv import load_dotenv
load_dotenv()

app = FastAPI()

MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client["nepal_location"]

class LocationValidationRequest(BaseModel):
    province_title: str
    district_title: str
    municipality_title: str
    ward_number: int

@app.post("/validate-location")
def validate_location(data: LocationValidationRequest):
    # 1. Check province exists
    province = db["province"].find_one({
        "province_title": {"$regex": f"^{data.province_title}$", "$options": "i"}
    })
    if not province:
        return {"valid": False, "message": "Province not found"}

    # 2. Check district belongs to province
    district = db["district"].find_one({
        "district_title": {"$regex": f"^{data.district_title}$", "$options": "i"},
        "province_id": province.get("id")
    })
    if not district:
        return {"valid": False, "message": "District not found or does not belong to province"}

    # 3. Check municipality belongs to district
    municipality = db["municipality"].find_one({
        "municipality_title": {"$regex": f"^{data.municipality_title}$", "$options": "i"},
        "district_id": district.get("id")
    })
    if not municipality:
        return {"valid": False, "message": "Municipality not found or does not belong to district"}

    # 4. Check ward belongs to municipality
    ward = db["ward"].find_one({
        "ward_number": data.ward_number,
        "municipality_id": municipality.get("id")
    })
    if not ward:
        return {"valid": False, "message": "Ward number not found or does not belong to municipality"}

    return {"valid": True, "message": "Location hierarchy is valid"}
