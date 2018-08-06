from lxml import html
from database import model_mongodb
from datetime import datetime
import eikonLib as ekLib
import requests, sys
import eikon as ek
import pandas as pd
import collections

#ek.set_app_id('9FB32FA719C8F1EE8CDEF1A')
ek.set_app_id('80a60244246c4c139ea016a0c9dde616194983de')
#pd.set_option('display.max_columns', 10000)
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
            #aBusinessSummary = ekLib.get_business_summary(aEikonTicker)
            #aEikonAllData.update(aBusinessSummary)

            #Competitors
            #aCompetitors = ekLib.get_competitors(aEikonTicker)
            #aEikonAllData.update(aCompetitors)

            #52 week high/low prices
            #a52WeekHighLow = ekLib.get_52_week_high_low(aEikonTicker)
            #aEikonAllData.update(a52WeekHighLow)

            #Beta info
            #aListBetas=ekLib.get_betas(aEikonTicker)
            #aEikonAllData.update(aListBetas)

            #Monthly share price updates
            #aListMonthlyUpdates=ekLib.get_120_month_share_price(aEikonTicker)
            #aEikonAllData.update(aListMonthlyUpdates)

            # Historic daily price for past year
            #aListHistoricDailyPrices=ekLib.get_365_day_share_price(aEikonTicker)
            #aEikonAllData.update(aListHistoricDailyPrices)

            #Daily price, volume, EV, market cap
            aListDailyUpdates=ekLib.get_daily_updates(aEikonTicker)
            aEikonAllData.update(aListDailyUpdates)

            #Major shareholders from CNN
            #aMajorOwners=get_major_shareholders_cnn(aEikonTicker)
            #aEikonAllData.update(aMajorOwners[0])
            #aEikonAllData.update(aMajorOwners[1])

            #Major shareholders
            #aMajorOwners=ekLib.get_major_shareholders(aEikonTicker)
            #aEikonAllData.update(aMajorOwners)

            #Fiscal Year end date
            aFiscalYearEndDate=ekLib.get_fiscal_year_dates(aEikonTicker)
            aEikonAllData.update(aFiscalYearEndDate)

            #Fiscal year data
            #aFiscalYearData=ekLib.get_all_year_data(aEikonTicker)
            #aEikonAllData.update(aFiscalYearData)

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
    wrongTickers=[]
    for aTicker in aEikonTickers:
        try:
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
    f= open("guru99.txt","w+")
    for aTicker in aEikonTickers:
        try:
            data = iModel.read_by_ticker(aTicker)
            ticker = aTicker
            print(data["DataByFiscalYear"])
            per2017 = data["DataByFiscalQuarter"]["FY2017Q4"]["EPS"]
            per2017 = data["DataByFiscalQuarter"]["FY2017Q3"]["EPS"]
            per2017 = data["DataByFiscalQuarter"]["FY2017Q2"]["EPS"]
            per2017 = data["DataByFiscalQuarter"]["FY2017Q1"]["EPS"]
            per2015 = data["DataByFiscalYear"]["FY2017"]["EPS"]
            eps2015 = data["DataByFiscalYear"]["FY2015"]["EPS"]
            eps2014 = data["DataByFiscalYear"]["FY2015"]["EPS"]
            price2017 = data["120MonthSharePrice"]["2017-09"]
            price2014 = data["120MonthSharePrice"]["2014-09"]
            a= aTicker+","+eps2017+","+eps2014+","+price2017+","+price2014+"\n"
            print(a)
            f.write(aTicker+","+eps2017+","+eps2014+","+price2017+","+price2014+"\n")
        except KeyboardInterrupt:
            sys.exit(0)
        except Exception as e:
            wrongTickers.append(aTicker)
            print(wrongTickers)
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
    file = 'SP500.txt'
    aEikonTickers=retrieve_eikon_file(file)

    #delete_ticker_data(model,["BetaWklyUp3Y","DailyUpdated"],aEikonTickers)
    get_all_eikon_data(model,aEikonTickers,file)
    #ekLib.retrieve_fiscal_year_data('AMA.MC')
    #ekLib.get_bonds('AAPL.O')
    #update_ticker_function(model, ekLib.retrieve_fiscal_year_data, aEikonTickers)
    #earnings_power(model,aEikonTickers)
    #print(wrongTickers)
    #print(get_competitors('AAPL.O'))


