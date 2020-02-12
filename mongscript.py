from pymongo import MongoClient
from bson.raw_bson import RawBSONDocument
from bson.codec_options import CodecOptions

client = MongoClient()

codec_options = CodecOptions(document_class=RawBSONDocument)
client = MongoClient('mongodb://localhost:27017')

db = client['jacaransa-db']


def aggregationmethod(collection):
    aggregation_conditions = [{'$group': {
        '_id': {'district': '$district',
                "price['value']": '$price.value',
                "price['currency']": '$price.currency'},
        'uniqueIds': {'$push': '$_id'},
        'details': {'$addToSet': {'title': '$title',
                                  'district': '$district', 
                                  'price': '$price'}},
        'count': {'$sum': 1},
    }}, {'$match': {'count': {'$gt': 1}}}, {'$project': {'_id': 0,
                                                         'uniqueIds': 1,
                                                         'details': 1}
}]

    results = collection.aggregate(aggregation_conditions)
    mycol = db['customers']
    mycol.insert_many(results)


formatted_properties = db.formatted_properties

aggregationmethod(formatted_properties)
