from pymongo import MongoClient
from bson.raw_bson import RawBSONDocument
from bson.codec_options import CodecOptions

client = MongoClient()

codec_options = CodecOptions(document_class=RawBSONDocument)
client = MongoClient('mongodb://localhost:27017')


db = client['jacaranda-db']


def aggregationmethod(collection):
    aggregation_conditions = [ 
    {"$group": {
            "_id": {"district": "$district","price['value']":"$price.value","price['currency']":"$price.currency"},
            "name":{"$addToSet":"$title"},
            "_id":{"addToSet": "$_id"},
            "uniqueIds": {"$push": "$_id"},
            "count":{"$sum":1}
            }
        },
        {"$match": { 
            "count": {"$gt": 1}
            }
        },{
        "$project":{
            "_id":0,
            "uniqueIds":1,
            "name":1
        }
        }]

    results = collection.aggregate(aggregation_conditions)
    mycol = mydb["customers"]
    mycol.insert_many(results)



formatted_properties = db.formatted_properties


aggregationmethod(formatted_properties)


