import sys
sys.path.append("..")
from database import model_mongodb
from lxml import html
from datetime import datetime
import eikonLib as ekLib
import visibleAlphaLib as vaLib
import requests, sys
import eikon as ek
import pandas as pd
import collections

ek.set_app_id('80a60244246c4c139ea016a0c9dde616194983de')
pd.options.display.max_colwidth = 10000

def retrieve_eikon_file(iFileName):
    # load json with S&P 500 companies#
    with open('./Tickers/'+iFileName,'r') as f:
        tickers = f.readlines()
    tickers = [x.strip() for x in tickers]
    return tickers

def get_major_shareholders_cnn(iEikonTicker):
    print("major shareholders")
    #Init dictionaries
    oMajorOwners={"Top-10-Owners":{}}
    oMajorFunds={"Top-10-Mutual-Funds":{}}
    aTopOwnersDict={}
    aTopFundsDict={}
    iSimplifiedTicker=ekLib.eikon_to_regular_ticker(iEikonTicker)

    #Request page and parse
    page = requests.get('http://money.cnn.com/quote/shareholders/shareholders.html?symb='+str(iSimplifiedTicker)+'&subView=institutional')
    #Todo sleep to give time to requests.get to come back
    #time.sleep(0.5)
    try:
        tree = html.document_fromstring(page.content)
    except:
        print("caught exception try again")
        page = requests.get('http://money.cnn.com/quote/shareholders/shareholders.html?symb='+str(iSimplifiedTicker)+'&subView=institutional')
        tree = html.document_fromstring(page.content)
    for x in range (1,11):
        aPercDict1={}
        aPercDict2={}
        aTopOwner = tree.xpath('//*[@id="wsod_shareholders"]/table[2]/tbody/tr['+str(x)+']/td[1]/span')
        if (len(aTopOwner)>0):
            #Extract top 5 owners and mutual funds percentages of ownership
            aTopOwnerPerc = tree.xpath('//*[@id="wsod_shareholders"]/table[2]/tbody/tr['+str(x)+']/td[2]')
            aFund = tree.xpath('//*[@id="wsod_shareholders"]/table[3]/tbody/tr['+str(x)+']/td[1]/span')
            aFundPerc = tree.xpath('//*[@id="wsod_shareholders"]/table[3]/tbody/tr['+str(x)+']/td[2]')

            #Top Owners names and percentages, names cant have ., or db crashes
            aTopOwnerName=aTopOwner[0].attrib.get('title').replace(".","")
            aTopOwnerPerc=ekLib.FloatOrZero(aTopOwnerPerc[0].text_content().replace("%",""))
            aPercDict1["Percentage"]=aTopOwnerPerc
            aTopOwnersDict[aTopOwnerName]=aPercDict1

            #Top Funds names and percentages
            aTopaFundName=aFund[0].attrib.get('title').replace(".","")
            aTopFundPerc=FloatOrZero(aFundPerc[0].text_content().replace("%",""))
            aPercDict2["Percentage"]=aTopFundPerc
            aTopFundsDict[aTopaFundName]=aPercDict2

    oMajorOwners["Top-10-Owners"]=aTopOwnersDict
    oMajorFunds["Top-10-Mutual-Funds"]=aTopFundsDict
    return [oMajorOwners,oMajorFunds]

def get_all_eikon_data(aMongoDBModel,iEikonTickers,file):
    aEikonExeptList=[]
    aEikonAllData={}
    for aEikonTicker in iEikonTickers:
        try:
            print(aEikonTicker)

            #Timestamp
            aEikonAllData["LastModified"] = datetime.utcnow()

            #Convector Index
            aEikonAllData["ConvectorIndex"] = file.replace('.txt','')

            #Tickers
            aEikonAllData["EikonTicker"] = aEikonTicker
            aEikonAllData["Ticker"] = ekLib.eikon_to_regular_ticker(aEikonTicker)

            #Company name
            aCompanyName = ekLib.get_common_name(aEikonTicker)
            aEikonAllData.update(aCompanyName)

            #Company description
            aBusinessSummary = ekLib.get_business_summary(aEikonTicker)
            aEikonAllData.update(aBusinessSummary)

            #Competitors
            aCompetitors = ekLib.get_competitors(aEikonTicker)
            aEikonAllData.update(aCompetitors)

            #52 week high/low prices
            a52WeekHighLow = ekLib.get_52_week_high_low(aEikonTicker)
            aEikonAllData.update(a52WeekHighLow)

            #Beta info
            aListBetas=ekLib.get_betas(aEikonTicker)
            aEikonAllData.update(aListBetas)

            #Monthly share price updates
            aListMonthlyUpdates=ekLib.get_120_month_share_price(aEikonTicker)
            aEikonAllData.update(aListMonthlyUpdates)

            # Historic daily price for past year
            aListHistoricDailyPrices=ekLib.get_365_day_share_price(aEikonTicker)
            aEikonAllData.update(aListHistoricDailyPrices)

            #Daily price, volume, EV, market cap
            aListDailyUpdates=ekLib.get_daily_updates(aEikonTicker)
            aEikonAllData.update(aListDailyUpdates)

            #Major shareholders from CNN
            #aMajorOwners=get_major_shareholders_cnn(aEikonTicker)
            #aEikonAllData.update(aMajorOwners[0])
            #aEikonAllData.update(aMajorOwners[1])

            #Major shareholders
            aMajorOwners=ekLib.get_major_shareholders(aEikonTicker)
            aEikonAllData.update(aMajorOwners)

            #Fiscal Year end date
            aFiscalYearEndDate=ekLib.get_fiscal_year_dates(aEikonTicker)
            aEikonAllData.update(aFiscalYearEndDate)

            #Fiscal year data
            aFiscalYearData=ekLib.get_all_year_data(aEikonTicker)
            aEikonAllData.update(aFiscalYearData)

            #Fiscal quarter data
            aFiscalQuarterData=ekLib.retrieve_fiscal_quarter_data(aEikonTicker)
            aEikonAllData.update(aFiscalQuarterData)

            model_mongodb.add_to_mongo(aMongoDBModel,aEikonAllData)
            aEikonAllData.clear()
            print(aEikonExeptList)
        except KeyboardInterrupt:
            sys.exit(0)
        except Exception as e:
            aEikonExeptList.append(aEikonTicker)
            print(aEikonExeptList)
            print(e)
            continue

def update_ticker_function(iModel, iEikonFunction, aEikonTickers):
    from database import model_mongodb
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

def earnings_power(iModel, aEikonTickers):
    wrongTickers=[]
    #file = open("guru99.csv","w")
    eps_data={}
    for aTicker in aEikonTickers:
        try:
            dividends = epsL12M = eps3Y = epsExtraL12M = epsExtra3Y = 0
            data = iModel.read_by_ticker(aTicker)
            #Take the last 12 values that have estimated = false
            orderedData = collections.OrderedDict(sorted(data["DataByFiscalQuarter"].items()))
            fqs = [v for v in orderedData.values() if 'false' in v.values()][-16:]
            #price_now = fqs[-1]["Other"]["P/E"]*fqs[-1]["IncomeStatement"]["EPS"]
            #price_3Y_ago = fqs[3]["Other"]["P/E"]*fqs[3]["IncomeStatement"]["EPS"]

            date_now=fqs[-1]["PeriodEndDate"][:-3]
            price_now=data["120MonthSharePrice"][date_now]["PriceClose"]
            date_3Y=fqs[3]["PeriodEndDate"][:-3]
            price_3Y_ago=data["120MonthSharePrice"][date_3Y]["PriceClose"]

            #Sum dividends of last 3Y
            for idx in range(12):
                dividends += fqs[idx+4]["IncomeStatement"]["DPS"]

            #Sum EPS of last 12Months
            for idx in range(4):
                epsL12M += fqs[len(fqs)-1-idx]["IncomeStatement"]["EPS"]
                epsExtraL12M += fqs[len(fqs)-1-idx]["IncomeStatement"]["EPSInclExtra"]

            #Sum EPS of 12 months 3Y ago
            for idx in range(4):
                eps3Y += fqs[idx]["IncomeStatement"]["EPS"]
                epsExtra3Y += fqs[idx]["IncomeStatement"]["EPSInclExtra"]

            a= aTicker+','+str(price_now)+','+str(price_3Y_ago)+','+str(dividends)+','+str(epsL12M)+','+str(eps3Y)+','+str(epsExtraL12M)+','+str(epsExtra3Y)
            #print(a)
            file.write(a+"\n")
        except KeyboardInterrupt:
            sys.exit(0)
        except Exception as e:
            wrongTickers.append(aTicker)
            print(wrongTickers)
            print(e)
            continue
    file.close()

def storeVisibleAlpha(aMongoDBModel,iEikonTickers):
    aEikonExeptList=[]
    aVisibleAlphaAllData=collections.defaultdict(dict)
    token = vaLib.getAuthToken()
    for aEikonTicker in iEikonTickers:
        try:
            print(aEikonTicker)
            regularTicker = str(aEikonTicker).split('.')[0]
            ticker = 'ticker='+regularTicker+'&'
            vaDataQuarter = vaLib.getBulkForTicker(aEikonTicker, token, False)
            aVisibleAlphaAllData['EikonTicker']=str(aEikonTicker)
            aVisibleAlphaAllData['Ticker']=str(regularTicker)
            aVisibleAlphaAllData.update(vaDataQuarter)
            vaDataYear = vaLib.getBulkForTicker(aEikonTicker, token, True)
            aVisibleAlphaAllData.update(vaDataYear)
            model_mongodb.add_to_mongo(aMongoDBModel,dict(aVisibleAlphaAllData))
            aVisibleAlphaAllData.clear()
            print(aEikonExeptList)
        except KeyboardInterrupt:
            sys.exit(0)
        except Exception as e:
            aEikonExeptList.append(aEikonTicker)
            print(aEikonExeptList)
            print(e)
            continue

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

def delete_ticker_data(iModel, iKeyListToBeRemoved, aEikonTickers):
    from database import model_mongodb
    wrongTickers=[]
    for aTicker in aEikonTickers:
        try:
            data = iModel.read_by_ticker(aTicker)
            id = data['_id']
            for key in iKeyListToBeRemoved:
                try:
                    del data[key]
                    iModel.update(data,str(id))
                except KeyError:
                    pass
        except KeyboardInterrupt:
            sys.exit(0)
        except:
            wrongTickers.append(aTicker)
            print(wrongTickers)
            continue
    print(wrongTickers)
    return data

if __name__ == '__main__':
    model = model_mongodb

    #Eikon all tickers
    file = 'FTSE350.txt'
    aEikonTickers=retrieve_eikon_file(file)
    #storeVisibleAlpha(model,aEikonTickers)
    get_all_eikon_data(model,aEikonTickers,file)

    #delete_ticker_data(model,["BetaWklyUp3Y","DailyUpdated"],aEikonTickers)
    #ekLib.get_120_month_share_price(aEikonTickers)
    #ekLib.get_365_day_share_price('AAPL.O')
    #ekLib.retrieve_eikon_reports('AAPL.O','FY','5')
    #ekLib.get_bonds('AAPL.O')
    #update_ticker_function(model, ekLib.get_daily_updates, aEikonTickers)
    #earnings_power(model,aEikonTickers)
    #print(wrongTickers)
    #print(get_competitors('AAPL.O'))


