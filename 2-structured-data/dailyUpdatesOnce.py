import sys
sys.path.append("..")
from database import model_mongodb
import eikon as ek
import eikonLib as ekLib
import collections, math, copy
from datetime import datetime
ek.set_app_key('80a60244246c4c139ea016a0c9dde616194983de')
ek.set_timeout(120)
def retrieve_eikon_file(iFileName):
    with open('./Tickers/'+iFileName,'r') as f:
        tickers = f.readlines()
    tickers = [x.strip() for x in tickers]
    return tickers

def FloatOrZero(value):
    try:
        if math.isnan(value):
            return 0.0
        else:
            return float(value)
    except:
        return 0.0

def update_ticker_function(iModel, iEikonFunction, aEikonTickers):
    print('update')
    wrongTickers=[]
    for aTicker in aEikonTickers:
        try:
            print(aTicker)
            data = iModel.read_by_eikon_ticker(aTicker)
            id = data['_id']
            if iEikonFunction.__name__ is "get_365_day_share_price":
                # Fill in missing days only by getting latest day available and comparing with today
                dailyPrices = data["365DaySharePrice"]
                date = sorted(dailyPrices.keys())[-1]
                dateobj = datetime.strptime(date, '%Y-%m-%d')
                delta = datetime.now() - dateobj
                if delta.days > 0:
                    # Watch out, we request based on delta.days number i.e: 10, but actually eikon returns a number of
                    # elements which could be bigger than the number of days (since there is no trading on weekends).
                    # 10 elements could be up to 14 weekdays.
                    #
                    # Example. If its a monday and i last had data on friday i will delta = 3 elements (sat + sun + mon)
                    # which go back to monday+friday+thursday, but i already have data from thu and fri, so i only want
                    # to pop 1 element. Eikon gives me from date 0 to -x days both inclusive, thats why we do x-1 to
                    # know how many items we pop.
                    updatedData=iEikonFunction(aTicker, str(delta.days-1))

                    # Calculate overlap between retrieved elements and existing elements (because we later need to pop
                    # using the number of unique new elements to add not the number of days)
                    duplicateItems = 0;
                    for item in updatedData['365DaySharePrice'].keys():
                        if item in dailyPrices:
                            duplicateItems += 1

                    #Purge old dates
                    dailyPrices = collections.OrderedDict(sorted(dailyPrices.items(), reverse=True))
                    counter=0
                    for idx in range(delta.days - duplicateItems):
                        counter+=1
                        dailyPrices.popitem(last=True)
                    data["365DaySharePrice"]= dict(dailyPrices)
                else:
                    print("Nothing to update in daily prices")
            else:
                updatedData=iEikonFunction(aTicker)
            updatedData["LastModified"]= datetime.utcnow()
            outdata = recursive_update(data,updatedData)
            iModel.update(outdata,str(id))
        except KeyboardInterrupt:
            sys.exit(0)
        except Exception as e:
            wrongTickers.append(aTicker)
            print(wrongTickers)
            print(e)
            continue
    print(wrongTickers)
    return data

def recursive_update(data, update):
    for k, v in update.items():
        # if data is a map
        if isinstance(data, collections.Mapping):
            # and if values of update are a map
            if isinstance(v, collections.Mapping):
                # call recursive update with submap (go down 1 level into map)
                r = recursive_update(data.get(k, {}), v)
                data[k] = r
            else:
                # when the data is no longer a mapping, get the same key from update and set data
                data[k] = update[k]
        else:
            # if not collection replace data with update
            data = {k: update[k]}
    data = dict(collections.OrderedDict(sorted(data.items(), reverse=True)))
    return data

if __name__ == '__main__':
    model = model_mongodb
    file = 'DailyUpdated.txt'
    aEikonTickers=retrieve_eikon_file(file)
    update_ticker_function(model, ekLib.get_365_day_share_price, aEikonTickers)