from pymongo import MongoClient

client = MongoClient()


client = MongoClient('mongodb://localhost:27017')


db = client['jacaransa-db']


def aggregationmethod(collection):
    aggregation_conditions = [ 
    {"$group": {
            "_id": {"district": "$district","price['value']":"$price.value","price['currency']":"$price.currency"},
            "uniqueIds": {"$push": "$_id"},
            "count":{"$sum":1}
            }
        },
        {"$match": { 
            "count": {"$gt": 1}
            }
        },
        {
            "$merg"
        },{
        "$project":{
            "_id":0,
            "uniqueIds":1
        }
        }]

    results = collection.aggregate(aggregation_conditions)

    for r in results:
    #    data = collection.aggregate([
    #         {"$match":{
    #             "_id":{"$in":r.uniqueIds}
    #         }}
    #     ])
       print(r)



formatted_properties = db.formatted_properties


aggregationmethod(formatted_properties)


