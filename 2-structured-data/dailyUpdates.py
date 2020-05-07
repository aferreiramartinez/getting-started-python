import sys
sys.path.append("..")
from database import model_mongodb
import eikon as ek
import eikonLib as ekLib
import collections
from datetime import datetime
ek.set_app_key('80a60244246c4c139ea016a0c9dde616194983de')

def retrieve_eikon_file(iFileName):
    # load json with S&P 500 companies#
    with open('./Tickers/'+iFileName,'r') as f:
        tickers = f.readlines()
    tickers = [x.strip() for x in tickers]
    return tickers

def update_ticker_function(iModel, iEikonFunction, aEikonTickers):
    print('update')
    wrongTickers=[]
    for aTicker in aEikonTickers:
        try:
            print(aTicker)
            data = iModel.read_by_ticker(aTicker)
            id = data['_id']
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

def recursive_update(d,u):
    for k, v in u.items():
        if isinstance(d, collections.Mapping):
            if isinstance(v, collections.Mapping):
                r = recursive_update(d.get(k, {}), v)
                d[k] = r
            else:
                d[k] = u[k]
        else:
            d = {k: u[k]}
    return d

if __name__ == '__main__':
    model = model_mongodb
    file = 'DailyUpdated.txt'
    aEikonTickers=retrieve_eikon_file(file)
    update_ticker_function(model, ekLib.get_daily_updates, aEikonTickers)