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

import logging
from lxml import html
import http.client, urllib.request, urllib.parse, urllib.error, base64, json, time, datetime, math, requests, calendar,copy, sys
import eikon as ek
import pandas as pd
from pprint import pprint
from datetime import datetime
from flask import current_app, Flask, redirect, url_for

# operations https://services.last10k.com/v1/company/{ticker}/operations?formType=10-K&filingOrder=0
# liabilities https://services.last10k.com/v1/company/{ticker}/liabilities?formType=10-K&filingOrder=0
# stock-quote https://services.last10k.com/v1/company/VIAB/quote
# conn.request("GET", "/v1/company/latestfilings?%s" % params, "{body}", headers) https://services.last10k.com/v1/company/latestfilings[?formType]
ek.set_app_id('9FB32FA719C8F1EE8CDEF1A')
#pd.set_option('display.max_columns', 10000)
pd.options.display.max_colwidth = 10000

def FloatOrZero(value):
    try:
        if math.isnan(value):
            return 0.0
        else:
            return float(value)
    except:
        return 0.0

def retrieve_eikon_file(iFileName):
    # load json with S&P 500 companies#
    with open('./Tickers/'+iFileName,'r') as f:
        tickers = f.readlines()
    tickers = [x.strip() for x in tickers]
    return tickers

def set_last10k_req_headers(iIndex):

    # Request headers
    #ant1bball
    #'Ocp-Apim-Subscription-Key': '4137e8075de949d49d3cd644cd81b884'
    #leo.fercom1
    #'Ocp-Apim-Subscription-Key': '999bcdb357474cf8bb325e75ef19666d'

    pool_api_keys=['4137e8075de949d49d3cd644cd81b884', '999bcdb357474cf8bb325e75ef19666d']
    headers = {
        # Request headers
        'Ocp-Apim-Subscription-Key': pool_api_keys[iIndex]
    }
    return headers

def set_last10k_req_params(iFormType,iFilingOrder):
    params = urllib.parse.urlencode({
        # Request parameters
        'formType': iFormType,
        'filingOrder': iFilingOrder,
    })
    return params

def get_last10k_form_name(iFormType):
    now = datetime.datetime.now()
    if (iFormType == '10-K'):
        return  str(now.year-1)+'-10K'
    elif (iFormType == '10-Q'):
        currentQuarter = math.ceil(now.month/3.)
        return  str(now.year-1)+'-10Q-Q'+str(currentQuarter)

def eikon_to_regular_ticker(iEikonTicker):
    regularTicker=iEikonTicker.split('.', 1)[0]
    #Todo check if the second split is needed
    regularTicker=regularTicker.split('.', 1)[0]
    print("regular ticker"+regularTicker)
    return regularTicker

def get_business_summary(iEikonTicker):
    aBusinessSummaryJson={}
    aStartIndex=1
    aLabels=['CompanyDescription',
             'HQCountryCode',
             'GICSSector',
             'GICSIndustryGroup',
             'GICSIndustry',
             'GICSSubIndustry',
             'BusinessSectorScheme',
             'BusinessSector',
             'TradedInIdentifier',
             'MemberIndexRic',
             'PriceMainIndexRIC']
    df = ek.get_data(iEikonTicker,
                     ['TR.BusinessSummary',
                      'TR.HQCountryCode',
                      'TR.GICSSector',
                      'TR.GICSIndustryGroup',
                      'TR.GICSIndustry',
                      'TR.GICSSubIndustry',
                      'TR.BusinessSectorScheme',
                      'TR.BusinessSector',
                      'CF_EXCHNG',
                      'TR.MemberIndexRic',
                      'TR.PriceMainIndexRIC'],
                     raw_output=True)
    for business in aLabels:
        aBusinessSummaryJson[business]=df['data'][0][aStartIndex]
        aStartIndex+=1
    return aBusinessSummaryJson

def get_common_name(iEikonTicker):
    aNameJson={}
    df = ek.get_data(iEikonTicker, 'TR.CommonName',raw_output=True)
    aNameJson["CompanyName"]=df['data'][0][1]
    return aNameJson

def get_52_week_high_low(iEikonTicker):
    a52WHighLowJson={}
    df = ek.get_data(iEikonTicker, ['TR.Price52WeekHigh','TR.Price52WeekLow'], raw_output=True)
    a52WHighLowJson["Price52WeekHigh"]=FloatOrZero(df['data'][0][1])
    a52WHighLowJson["Price52WeekLow"]=FloatOrZero(df['data'][0][2])
    return a52WHighLowJson

def get_betas(iEikonTicker):
    print("betas")
    aBetasJson={"Betas":{}}
    aStartIndex=1
    aLabels=['BetaWkly3Y',
             'BetaWklyUp3Y',
             'BetaWklyDown3Y',
             'BetaWkly2Y',
             'BetaWklyUp2Y',
             'BetaWklyDown2Y']
    df = ek.get_data(iEikonTicker,
                     ['TR.BetaWkly3Y',
                      'TR.BetaWklyUp3Y',
                      'TR.BetaWklyDown3Y',
                      'TR.BetaWkly2Y',
                      'TR.BetaWklyUp2Y',
                      'TR.BetaWklyDown2Y'],
                     raw_output=True)
    for betas in aLabels:
        aBetasJson["Betas"][betas]=FloatOrZero(df['data'][0][aStartIndex])
        aStartIndex+=1
    return aBetasJson

def get_30_day_volume(iEikonTicker):
    aAccumulatedVol = 0
    volumes=ek.get_data(iEikonTicker, 'TR.ACCUMULATEDVOLUME(SDate=0,EDate=-29,Frq=D)',raw_output=True)
    for vol in volumes['data']:
        aAccumulatedVol += FloatOrZero(vol[1])
    return aAccumulatedVol

def get_365_day_share_price(iEikonTicker):
    print("365 daily price")
    o365DayPrice={"180DaySharePrice":{}}
    aJson={}
    initCounter=0
    aPrices=ek.get_data(iEikonTicker, ['TR.PriceClose(SDate=0,EDate=-364,Frq=D)',
                                       'TR.PriceHigh(SDate=0,EDate=-364,Frq=D)',
                                       'TR.PriceLow(SDate=0,EDate=-364,Frq=D)'],raw_output=True)
    for aPrice in aPrices['data']:
        aJson["PriceClose"]=FloatOrZero(aPrice[1])
        aJson["PriceHigh"]=FloatOrZero(aPrice[2])
        aJson["PriceLow"]=FloatOrZero(aPrice[3])
        o365DayPrice["180DaySharePrice"]["D-"+str(initCounter)]=copy.deepcopy(aJson)
        initCounter +=1
    return o365DayPrice

def get_120_month_share_price(iEikonTicker):
    print("monthly updates")
    o120MonthPrice={"120MonthSharePrice":{}}
    aJson={}
    initCounter=0
    aPrices=ek.get_data(iEikonTicker,'TR.PriceClose(SDate=0,EDate=-119,Frq=CM)',raw_output=True)

    for aPrice in aPrices['data']:
        aJson["PriceClose"]=FloatOrZero(aPrice[1])
        o120MonthPrice["120MonthSharePrice"]["M-"+str(initCounter)]=copy.deepcopy(aJson)
        initCounter +=1
    return o120MonthPrice

def get_daily_updates(iEikonTicker):
    oDailyJson={"DailyUpdated":{}}
    aStartIndex=1
    aLabels=['CompanyMarketCap',
             'EV',
             'SharePrice',
             'aDailyVolume']
    df = ek.get_data(iEikonTicker, ['TR.CompanyMarketCap','TR.EV','CF_LAST','TR.Volume'], raw_output=True)
    for data in aLabels:
        oDailyJson["DailyUpdated"][data] = FloatOrZero(df['data'][0][aStartIndex])
        aStartIndex+=1
    oDailyJson["DailyUpdated"]["30DayVolume"]=get_30_day_volume(iEikonTicker)
    return oDailyJson

def get_competitors(iEikonTicker):
    print("competitors")
    aIndex=1
    oDailyJson={"Competitors":{}}
    aJson={}
    screener_exp = "SCREEN(U(IN(Peers('IBM.N'))))"
    peers = ek.get_data(instruments=[screener_exp], fields=['TR.CompanyName'],raw_output=True)
    for company in peers["data"]:
        aJson["name"]=company[1]
        oDailyJson["Competitors"]["Competitor-"+str(aIndex)]=copy.deepcopy(aJson)
        aIndex += 1
    return oDailyJson

def get_minority_interest(iEikonTicker):
    print("min interest")
    aMinInterest={}
    df = ek.get_data(iEikonTicker, 'TR.MinorityInterestNonRedeemable', raw_output=True)
    aMinInterest["MinorityInterest"]=FloatOrZero(df['data'][0][1])
    return aMinInterest

def get_fiscal_year_dates(iEikonTicker):
    print("FY dates")
    aFY={}
    df = ek.get_data(iEikonTicker,'TR.EBITDA(Period=FY0).periodenddate',raw_output=True)
    month=df['data'][0][1].split("-")[1].lstrip("0")
    aMonthName=calendar.month_abbr[int(month)].upper()
    aMonthDay=df['data'][0][1].split("-")[2]
    aFY["FYEndDate"]=(aMonthDay+'-'+aMonthName)
    return aFY

def get_major_shareholders_cnn(iEikonTicker):
    print("major shareholders")
    #Init dictionaries
    aMajorOwners={"Top-10-Owners":{}}
    aMajorFunds={"Top-10-Mutual-Funds":{}}
    aTopOwnersDict={}
    aTopFundsDict={}
    iSimplifiedTicker=eikon_to_regular_ticker(iEikonTicker)

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
            aTopOwnerPerc=FloatOrZero(aTopOwnerPerc[0].text_content().replace("%",""))
            aPercDict1["Percentage"]=aTopOwnerPerc
            aTopOwnersDict[aTopOwnerName]=aPercDict1

            #Top Funds names and percentages
            aTopaFundName=aFund[0].attrib.get('title').replace(".","")
            aTopFundPerc=FloatOrZero(aFundPerc[0].text_content().replace("%",""))
            aPercDict2["Percentage"]=aTopFundPerc
            aTopFundsDict[aTopaFundName]=aPercDict2

    aMajorOwners["Top-10-Owners"]=aTopOwnersDict
    aMajorFunds["Top-10-Mutual-Funds"]=aTopFundsDict
    return [aMajorOwners,aMajorFunds]

def get_major_shareholders(iEikonTicker):
    maxShareholders = 10
    oMajorOwners = {"Top-10-Owners":{}}
    aJson={}
    aHolders = ek.get_data(instruments=['AAPL.O'], fields=['TR.InvestorFullName',
                                                           'TR.SharesHeld',
                                                           'TR.PctOfSharesOutHeld'],raw_output=True)
    for aHolder in aHolders["data"]:
        if maxShareholders >= 0:
            maxShareholders -= 1
            aJson["Percentage"]=FloatOrZero(aHolder[3])
            aJson["SharesOutstandingHeld"]=FloatOrZero(aHolder[2])
            oMajorOwners["Top-10-Owners"][aHolder[1].replace(".","")]=copy.deepcopy(aJson)
        else:
            break
    return oMajorOwners

def get_all_eikon_data(aMongoDBModel,iEikonTickers):
    aEikonExeptList=[]
    aEikonAllData={}
    for aEikonTicker in iEikonTickers:
        try:
            print(aEikonTicker)

            #Tickers
            aEikonAllData["EikonTicker"]=aEikonTicker
            aEikonAllData["Ticker"]=eikon_to_regular_ticker(aEikonTicker)

            #Company name
            aCompanyName = get_common_name(aEikonTicker)
            aEikonAllData.update(aCompanyName)

            #Company description
            aBusinessSummary = get_business_summary(aEikonTicker)
            aEikonAllData.update(aBusinessSummary)

            #Competitors
            aCompetitors = get_competitors(aEikonTicker)
            aEikonAllData.update(aCompetitors)

            #52 week high/low prices
            a52WeekHighLow = get_52_week_high_low(aEikonTicker)
            aEikonAllData.update(a52WeekHighLow)

            #Beta info
            aListBetas=get_betas(aEikonTicker)
            aEikonAllData.update(aListBetas)

            #Monthly share price updates
            aListMonthlyUpdates=get_120_month_share_price(aEikonTicker)
            aEikonAllData.update(aListMonthlyUpdates)

            # Historic daily price for past year
            aListHistoricDailyPrices=get_365_day_share_price(aEikonTicker)
            aEikonAllData.update(aListHistoricDailyPrices)

            #Daily price, volume, EV, market cap
            aListDailyUpdates=get_daily_updates(aEikonTicker)
            aEikonAllData.update(aListDailyUpdates)

            #Minority Interest
            #aMinInterest=get_minority_interest(aEikonTicker)
            #aEikonAllData.update(aMinInterest)

            #Major shareholders from CNN
            #aMajorOwners=get_major_shareholders_cnn(aEikonTicker)
            #aEikonAllData.update(aMajorOwners[0])
            #aEikonAllData.update(aMajorOwners[1])

            #Major shareholders
            aMajorOwners=get_major_shareholders(aEikonTicker)
            aEikonAllData.update(aMajorOwners)

            #Fiscal Year end date
            aFiscalYearEndDate=get_fiscal_year_dates(aEikonTicker)
            aEikonAllData.update(aFiscalYearEndDate)

            #Fiscal year data
            aFiscalYearData=retrieve_fiscal_year_data(aEikonTicker)
            aFiscalYearEstimatesData=retrieve_estimated_fiscal_year_data(aEikonTicker)
            aEikonAllData["DataByFiscalYear"]=aFiscalYearData
            aEikonAllData["DataByFiscalYear"].update(aFiscalYearEstimatesData)

            #Fiscal quarter data
            aFiscalQuarterData=retrieve_fiscal_quarter_data(aEikonTicker)
            aEikonAllData["DataByFiscalQuarter"]=(aFiscalQuarterData)

            add_to_mongo(aMongoDBModel,aEikonAllData)
            aEikonAllData.clear()
            print(aEikonExeptList)
        except KeyboardInterrupt:
            sys.exit(0)
        except:
            aEikonExeptList.append(aEikonTicker)
            print(aEikonExeptList)
            continue

def retrieve_eikon_reports(iEikonTicker, iPeriod):
    aLabels=['EBITDA',
             'EBIT',
             'TotalRevenue',
             'EPS',
             'DilSharesOut',
             'CashAndSTInv',
             'TotalDebt',
             'MinorityInterest',
             'DPS',
             'EV/EBITDA',
             'EV/EBIT',
             'NetDebt',
             'EV']

    df = ek.get_data(iEikonTicker,
                     ['TR.EBITDAActValue(Period='+iPeriod+')',
                      'TR.EBITActValue(Period='+iPeriod+')',
                      'TR.TotalRevenue(Period='+iPeriod+')',
                      'TR.EPSActValue(Period='+iPeriod+')',
                      'TR.TtlCmnSharesOut(Period='+iPeriod+')',
                      'TR.CashAndSTInvestments(Period='+iPeriod+')',
                      'TR.TotalDebtOutstanding(Period='+iPeriod+')',
                      'TR.MinorityInterestNonRedeemable(Period='+iPeriod+')',
                      'TR.DpsCommonStock(Period='+iPeriod+')',
                      'TR.HistEnterpriseValueEBITDA(Period='+iPeriod+')',
                      'TR.EVEBIT(Period='+iPeriod+')',
                      'TR.NetDebt(Period='+iPeriod+')',
                      'TR.HistEnterpriseValue(Period='+iPeriod+')'],
                     raw_output=True)
    return [aLabels,df]

def retrieve_fiscal_year_data(iEikonTicker):
    print("retrieve_fiscal_year_data")
    aFiscalYears=['FY-2','FY-1','FY0']
    aFYDataDict={}
    oListJson={}
    aFYDataDict["Estimated"]="false"

    #Obtain last reported Fiscal Year date
    df = ek.get_data(iEikonTicker,'TR.EBITDA(Period=FY0).periodenddate',raw_output=True)
    aLastFYEnd=df['data'][0][1].split("-")
    aLastFYEnd=datetime(int(aLastFYEnd[0]),int(aLastFYEnd[1].lstrip("0")),int(aLastFYEnd[2].lstrip("0")))
    #Start counting from most ancient year to avoid reverse order
    aFY0=aLastFYEnd.year-len(aFiscalYears)+1
    for fy in aFiscalYears:
        #NOTE:Historic fiscal year price close
        aLabels,df = retrieve_eikon_reports(iEikonTicker, fy)
        #Get array of all data
        aDfLen=len(df['data'][0])-1
        aFYJson={"FY"+str(aFY0):{}}
        for idx in range(0,aDfLen):
            aFYDataDict[aLabels[idx]]=FloatOrZero(df['data'][0][idx+1])
        aFYJson["FY"+str(aFY0)]=aFYDataDict
        oListJson.update(copy.deepcopy(aFYJson))
        aFY0=aFY0+1

    return oListJson

def retrieve_eikon_estimates(iEikonTicker, iPeriod):
    aLabels=["TotalRevenueMean",
             "EPSSmart",
             "EPSMean",
             "EVMean",
             "DPSMean",
             "DPSSmart",
             "EBITDASmart",
             "EBITDAMean",
             "EBITSmart",
             "EBITMean",
             "EV/EBITDASmart",
             "FwdEV/EBITDASmart",
             "FwdEV/EBITSmart",
             "NetDebtMean"]

    df = ek.get_data(iEikonTicker,
                     ['TR.RevenueMean(Period='+iPeriod+')',
                      'TR.EpsSmartEst(Period='+iPeriod+')',
                      'TR.EPSMean(Period='+iPeriod+')',
                      'TR.EVMean(Period='+iPeriod+')',
                      'TR.DPSMean(Period='+iPeriod+')',
                      'TR.DPSSmartEst(Period='+iPeriod+')',
                      'TR.EBITDASmartEst(Period='+iPeriod+')',
                      'TR.EBITDAMean(Period='+iPeriod+')',
                      'TR.EBITSmartEst(Period='+iPeriod+')',
                      'TR.EBITMean(Period='+iPeriod+')',
                      'TR.EVtoEBITDASmartEst(Period='+iPeriod+')',
                      'TR.FwdEVtoEBTSmartEst(Period='+iPeriod+')',
                      'TR.FwdEVtoEBISmartEst(Period='+iPeriod+')',
                      'TR.NetDebtMean(Period='+iPeriod+')'],
                     raw_output=True)
    return [aLabels,df]

def retrieve_estimated_fiscal_year_data(iEikonTicker):
    print("retrieve_estimated_fiscal_year_data")
    aFiscalYears=['FY1','FY2','FY3']
    aFYDataDict={}
    aFYDataDict["Estimated"]="true"
    oListJson={}
    #df = ek.get_data(iEikonTicker,'TR.EpsSmartEst(Period=FY1).fperiod',raw_output=True)
    df = ek.get_data(iEikonTicker,'TR.EpsSmartEst(Period=FY1).periodenddate',raw_output=True)
    aLastFYEnd=df['data'][0][1].split("-")
    aLastFYEnd=datetime(int(aLastFYEnd[0]),int(aLastFYEnd[1].lstrip("0")),int(aLastFYEnd[2].lstrip("0")))
    aFY1=aLastFYEnd.year

    for fy in aFiscalYears:
        #NOTE:Historic fiscal year price close
        aLabels,df = retrieve_eikon_estimates(iEikonTicker, fy)

        #Get array of all data, first parameter is ticker, it isnt needed
        aDfLen=len(df['data'][0])-1
        aFYJson={"FY"+str(aFY1):{}}
        for idx in range(0,aDfLen):
            aFYDataDict[aLabels[idx]]=FloatOrZero(df['data'][0][idx+1])
        aFYJson["FY"+str(aFY1)]=aFYDataDict
        oListJson.update(copy.deepcopy(aFYJson))
        #print(oListJson)
        aFY1=aFY1+1
    return oListJson

def retrieve_fiscal_quarter_data(iEikonTicker):
    print("quarterly data")
    aFiscalQuarters=['FQ-3','FQ-2','FQ-1','FQ0','FQ1','FQ2','FQ3','FQ4']
    aFQDataDict={}
    oListJson={}
    for fq in aFiscalQuarters:
        aFQDataDict.clear()
        if fq in aFiscalQuarters[0:4]:
            aFQDataDict["Estimated"]="false"
            aPeriod = ek.get_data(iEikonTicker,'TR.EBITActValue(Period='+fq+').fperiod',raw_output=True)
            aQuarter=aPeriod['data'][0][1]
            aLabels,df = retrieve_eikon_reports(iEikonTicker, fq)
        else:
            aFQDataDict["Estimated"]="true"
            aPeriod = ek.get_data(iEikonTicker,'TR.EpsSmartEst(Period='+fq+').fperiod',raw_output=True)
            aQuarter=aPeriod['data'][0][1]
            aLabels,df = retrieve_eikon_estimates(iEikonTicker, fq)
        #First elem of df is always the company name, we dont need it for len
        aDfLen=len(df['data'][0])-1
        aFYJson={str(aQuarter):{}}
        for idx in range(0,aDfLen):
            aFQDataDict[aLabels[idx]]=FloatOrZero(df['data'][0][idx+1])
        aFYJson[str(aQuarter)]=aFQDataDict
        oListJson.update(copy.deepcopy(aFYJson))
    #print(oListJson)
    return oListJson

def get_last10k_balance_sheet(iMDBModel, iFormType, numberFilingsBack):
    #Prepare request
    APIPoolIndex = 0
    headers = set_last10k_req_headers(APIPoolIndex)
    params = set_last10k_req_params(iFormType,numberFilingsBack)

    dataType = {"Balance Sheet":{}}
    dataYear = {get_last10k_form_name(iFormType):{}}
    errorList = []
    #TODO ADD SP500 FILE
    #companies = json.load(open('./bookshelf/SP500FILE','r'))
    conn = http.client.HTTPSConnection('services.last10k.com')
    for company in companies["SP500"]:
        conn.request("GET", "/v1/company/"+company["Ticker"]+"/balancesheet?%s" % params, "{body}", headers)
        response = conn.getresponse()
        print(str(response.status) + company["Ticker"])
        if (response.status == 403):
            print('Max queries detected, changing API key')
            conn.close()
            time.sleep(12)
            headers = set_last10k_req_headers(APIPoolIndex+1)
            conn = http.client.HTTPSConnection('services.last10k.com')
            conn.request("GET", "/v1/company/"+company["Ticker"]+"/balancesheet?%s" % params, "{body}", headers)
            response = conn.getresponse()

        if (response.status == 200):
            data = response.read().decode('utf-8')
            jsonData = json.loads(data)
            del jsonData['Content'] #deletes HTML content and URL to save space
            del jsonData['Url']
            dataType["Balance Sheet"]=jsonData
            dataYear["2016-10K"]=dataType
            company.update(dataYear)
            add_to_mongo(iMDBModel,company)
            time.sleep(12)
        else:
            print("error, skip "+str(company["Ticker"]))
            errorList.append(company["Ticker"])
            time.sleep(12)
            continue
    conn.close()

def create_app(config, debug=False, testing=False, config_overrides=None):

    app = Flask(__name__)
    app.config.from_object(config)

    app.debug = debug
    app.testing = testing

    if config_overrides:
        app.config.update(config_overrides)

    # Configure logging
    if not app.testing:
        logging.basicConfig(level=logging.INFO)

    # Setup the data model.
    with app.app_context():
        model = get_model()
        model.init_app(app)
        #Free last10k info
        #get_last10k_balance_sheet(model,'10-K','0')

        #Eikon all tickers
        aEikonTickers=retrieve_eikon_file('SP500.txt')
        get_all_eikon_data(model,aEikonTickers)
        #wrongTickers=[]
        #for aTicker in aEikonTickers:
        #    try:
        #        print(aTicker)
        #        update_eikon_ticker_mongo(model, get_daily_updates, aTicker)
        #        update_eikon_ticker_mongo(model, get_business_summary, aTicker)
        #    except KeyboardInterrupt:
        #        sys.exit(0)
        #    except:
        #        wrongTickers.append(aTicker)
        #        print(wrongTickers)
        #        continue
        #print(wrongTickers)
        #print(get_competitors('AAPL.O'))
        #get_business_summary('AAPL.O')


    # Register the Bookshelf CRUD blueprint.
    from .crud import crud

    # a blueprint is a set of operations which can be registered on an application
    # Flask associates view functions with blueprints when dispatching requests and
    # generating URLs from one endpoint to another
    app.register_blueprint(crud, url_prefix='/books')

    # Add a default root route.
    @app.route("/")
    def index():
        return redirect(url_for('crud.list'))

    # Add an error handler. This is useful for debugging the live application,
    # however, you should disable the output of the exception for production
    # applications.
    @app.errorhandler(500)
    def server_error(e):
        return """
        An internal error occurred: <pre>{}</pre>
        See logs for full stacktrace.
        """.format(e), 500
    return app


def get_model():
    model_backend = current_app.config['DATA_BACKEND']
    if  model_backend == 'mongodb':
        from . import model_mongodb
        model = model_mongodb
    else:
        raise ValueError(
            "No appropriate databackend configured. "
            "Please specify datastore, cloudsql, or mongodb")

    return model

def add_to_mongo(iModel,iData):
    #data = iData.to_dict(flat=True)
    #del iData['Content']
    from . import model_mongodb
    iModel.create(iData)
    print("added to mongo")

def update_eikon_ticker_mongo(iModel, iEikonFunction, iEikonTicker):
    from . import model_mongodb
    data = iModel.read_by_ticker(iEikonTicker)
    id = data['_id']
    updatedData=iEikonFunction(iEikonTicker)
    data.update(updatedData)
    iModel.update(data,str(id))
    return data

