from database import model_mongodb

if __name__ == '__main__':

    cursor = model_mongodb.read_all()
    counter=1
    for document in cursor:
        print(counter)
        counter+=1
        docId = document['_id']
        myList=document["observations"]
        document["observations"] = list(reversed(myList))
        model_mongodb.update(document, str(docId))



