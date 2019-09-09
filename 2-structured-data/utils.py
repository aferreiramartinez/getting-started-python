from database import model_mongodb

if __name__ == '__main__':
    #REVERSE OBSERVATIONS
    #cursor = model_mongodb.read_all()
    #counter=1
    #for document in cursor:
        #docId = document['_id']
        #myList=document["observations"]
        #document["observations"] = list(reversed(myList))
        #model_mongodb.update(document, str(docId))

    cursor = model_mongodb.read_all()
    with open('NASDAQ-ticker-and-names.txt', 'a') as the_file:
        for document in cursor:
            ticker=document["Ticker"]
            companyName=document["CompanyName"]
            the_file.write("'"+ticker+' - '+companyName+"',"+'\n')
        the_file.close()

