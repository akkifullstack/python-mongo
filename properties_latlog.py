from pymongo import MongoClient
from bson.raw_bson import RawBSONDocument
from bson.codec_options import CodecOptions
import requests
import json

client = MongoClient()

codec_options = CodecOptions(document_class=RawBSONDocument)

client = MongoClient('mongodb://localhost:27017')

db = client['jacaranda-db']


def propertiesLatLog(collection):
    mydoc = collection.aggregate([{
        "$group": {
            "_id": {
                "_id": "$_id",
                "country": "$country",
                "district": "$district"
            }
        }
    }, {
        "$project": {
            "_id": 1,
            "country": 1,
            "district": 1
        }
    }])

    res = [sub['_id'] for sub in mydoc]

    for r in res:
        dist = r['district']
        dist = dist.replace(" ", "+")
        country = r['country']
        api_key = 'AIzaSyAy1Z3e2qtLg7IvpEiMcObLfHUH9HrWcYE'
        url = 'https://maps.googleapis.com/maps/api/geocode/json?address=' + \
            dist+',+'+country+',+'+'&key='+api_key

        res_ob = requests.get(url)
        latlongJson = res_ob.json()
        resu = latlongJson['results']
        searchQuery = {"_id": r["_id"]}
        for latlong in resu:
            updateQuery = {"$set": {"property_location": latlong['geometry']}}

            # to update the collection to specific property

            x = collection.find_one_and_update(searchQuery, updateQuery)
            print(list(x))


collection = db.formatted_properties

propertiesLatLog(collection)
