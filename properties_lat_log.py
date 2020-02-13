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

    address = properties_result['address']
    address = address.replace(' ', '+')
    api_key = 'AIzaSyAy1Z3e2qtLg7IvpEiMcObLfHUH9HrWcYE'
    url = 'https://maps.googleapis.com/maps/api/geocode/json?address=' \
        + address + ',+' + '&key=' + api_key

    properties_location_obj = requests.get(url)
    lat_long_Json = properties_location_obj.json()
    return lat_long_Json


def properties_lat_log(collection):
    properties_doc = collection.find({'$or': [{'address': {'$ne': 'null'
            }, 'address': {'$exists': 'false'}}]}, {'address': 1,
            '_id': 1})

    for properties_result in properties_doc:

        # time.sleep(5)

        lat_long_result = google_map_api_call(properties_result)
        properties_lat_long_result = lat_long_result['results']
        search_query = {'_id': properties_result['_id']}
        for lat_long in properties_lat_long_result:
            update_query = {'$set': {'coordinates': lat_long['geometry'
                            ]['location']}}

            # to update the collection to specific property

            result = collection.find_one_and_update(search_query,
                    update_query, {
                '_id': 1,
                'title': 1,
                'coordinates': 1,
                'address': 1,
                })

            # result = collection.find_and_modify(search_query, update_query,{"new":"true"})

            print(result)


collection = db.formatted_properties

properties_lat_log(collection)

		
