# Copyright 2015 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from bson.objectid import ObjectId
from flask.ext.pymongo import PyMongo


builtin_list = list


mongo = PyMongo()


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


def init_app(app):
    mongo.init_app(app)


# [START list]
def list(limit=10, cursor=None):
    cursor = int(cursor) if cursor else 0

    results = mongo.db.books.find(skip=cursor, limit=10).sort('title')
    books = builtin_list(map(from_mongo, results))

    next_page = cursor + limit if len(books) == limit else None
    return (books, next_page)
# [END list]


# [START read]
def read(id):
    result = mongo.db.eikonTwo.find_one(_id(id))
    return from_mongo(result)
# [END read]

def read_by_ticker(iEikonTicker):
    result = mongo.db.eikonTwo.find_one({ "EikonTicker": iEikonTicker })
    return from_mongo(result)

def read_all():
    cursor = mongo.db.eikonTwo.find({})
    return from_mongo(cursor)

# [START create]
def create(data):
    new_id = mongo.db.eikonTwo.insert(data)
    return read(new_id)
# [END create]


# [START update]
def update(data, id):
    mongo.db.eikonTwo.update({'_id': _id(id)}, data)
    return read(id)
# [END update]


def delete(id):
    mongo.db.books.remove(_id(id))
