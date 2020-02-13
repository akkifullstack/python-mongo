from pymongo import MongoClient
from bson.raw_bson import RawBSONDocument
from bson.codec_options import CodecOptions
import requests
import json
import time

client = MongoClient()

codec_options = CodecOptions(document_class=RawBSONDocument)

client = MongoClient('mongodb://localhost:27017')

db = client['jacaranda-db']


def google_map_api_call(properties_result):
    if all(key in properties_result for key in ('country', 'district')):
        district = properties_result['district']
        district = district.replace(" ", "+")
        country = properties_result['country']
        api_key = 'AIzaSyAy1Z3e2qtLg7IvpEiMcObLfHUH9HrWcYE'
        url = 'https://maps.googleapis.com/maps/api/geocode/json?address=' + \
            district+',+'+country+',+'+'&key='+api_key
        # print(url)
    elif 'district' in properties_result:
        district = properties_result['district']
        district = district.replace(" ", "+")
        api_key = 'AIzaSyAy1Z3e2qtLg7IvpEiMcObLfHUH9HrWcYE'
        url = 'https://maps.googleapis.com/maps/api/geocode/json?address=' + \
            district+',+'+'&key='+api_key
        # print(url)
    elif 'country' in properties_result:
        country = properties_result['country']
        country = country.replace(" ", "+")
        api_key = 'AIzaSyAy1Z3e2qtLg7IvpEiMcObLfHUH9HrWcYE'
        url = 'https://maps.googleapis.com/maps/api/geocode/json?address=' + \
            country+',+'+'&key='+api_key
        # print(url)
        # print(url)

    properties_location_obj = requests.get(url)
    lat_long_Json = properties_location_obj.json()
    print(lat_long_Json)


def properties_lat_log(collection):
    my_doc = collection.aggregate([{
        "$group": {
            "_id": 0,
            "location": {
                "$addToSet": {
                    "country": '$country',
                    "district": "$district"
                }
            }
        }}, {
            "$project": {
                "_id": 0,
                "location": 1
            }
    }
    ])

    location = list(my_doc)
    properties = location[0]['location']

    for properties_result in properties:
        time.sleep(5)
        google_map_api_call(properties_result)

    # properties_lat_long_result = lat_long_Json['results']
    # search_query = {"_id": properties_result["_id"]}
    # for lat_long in properties_lat_long_result:
    #     update_query = {
    #         "$set": {"property_location": lat_long['geometry']['location']}
    #     }

    #     # to update the collection to specific property

    #     result = collection.find_one_and_update(
    #         search_query, update_query
    #     )


collection = db.formatted_properties

properties_lat_log(collection)
