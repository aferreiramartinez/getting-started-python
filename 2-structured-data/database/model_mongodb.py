from bson.objectid import ObjectId
from . import config
import pymongo

builtin_list = list
connection = pymongo.MongoClient(config.DB_HOST, config.DB_PORT)
db = connection[config.DB_NAME]
db.authenticate(config.DB_USER, config.DB_PASS)

def _id(id):
    if not isinstance(id, ObjectId):
        return ObjectId(id)
    return id


# [START from_mongo]
def from_mongo(data):
    """
    Translates the MongoDB dictionary format into the format that's expected
    by the application.
    """
    if not data:
        return None

    #data['id'] = str(data['_id'])
    return data
# [END from_mongo]


# # [START list]
# def list(limit=10, cursor=None):
#     cursor = int(cursor) if cursor else 0
#
#     results = mongo.db.books.find(skip=cursor, limit=10).sort('title')
#     books = builtin_list(map(from_mongo, results))
#
#     next_page = cursor + limit if len(books) == limit else None
#     return (books, next_page)
# # [END list]
#
# def delete(id):
#     mongo.db.books.remove(_id(id))


# [START read]
def read(id):
    result = db.eikonThree.find_one(_id(id))
    return from_mongo(result)
# [END read]

def read_by_ticker(iEikonTicker):
    result = db.eikonThree.find_one({ "EikonTicker": iEikonTicker })
    return from_mongo(result)

def read_all():
    cursor = db.eikonThree.find({})
    return from_mongo(cursor)

# [START create]
def create(data):
    print('create mongo')
    new_id = db.eikonThree.insert(data)
    return read(new_id)
# [END create]


# [START update]
def update(data, id):
    db.eikonThree.update({'_id': _id(id)}, data)
    return read(id)
# [END update]

def add_to_mongo(iModel,iData):
    #data = iData.to_dict(flat=True)
    #del iData['Content']
    print('add to mongo')
    iModel.create(iData)
    print("added to mongo")
