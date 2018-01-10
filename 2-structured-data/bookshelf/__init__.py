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
import http.client, urllib.request, urllib.parse, urllib.error, base64, json, time, datetime, math, requests, calendar,copy
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

def retrieve_sp_500_tickers():
    # load json with S&P 500 companies#
    data = json.load(open('./bookshelf/SP500.json','r'))
    tickers = []
    for company in data["SP500"]:
        tickers.append(company["Ticker"])
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
    regularTicker=regularTicker.split('.', 1)[0]

def isin_file_to_tickers():
    eikonTickers = []
    regularTickers = []
    with open("bookshelf/eikonIsin.txt") as file:
        for line in file:
            time.sleep(0.25)
            line = line.replace('\n','')
            ticker = ek.get_symbology(line,from_symbol_type='ISIN', to_symbol_type="RIC",raw_output=True)
            mappedSymb=ticker.get('mappedSymbols')
            firstmappedSymb=mappedSymb[0]
            (key, value), = firstmappedSymb.get('bestMatch').items()
            if key != 'error':
                eikonTickers.append(value)
                print(value)
            else:
                continue
    return eikonTickers

#def get_eikon_data():
#    tickers = isin_file_to_tickers()
#    thefile = open('test.txt', 'w')
#    for item in tickers:
#        regularTicker=item.split('.', 1)[0]
#        regularTicker=regularTicker.split('^', 1)[0]

def get_business_summary(iEikonTicker):
    df = ek.get_data(iEikonTicker, 'TR.BusinessSummary',raw_output=True)
    return df['data'][0][1]

def get_common_name(iEikonTicker):
    df = ek.get_data(iEikonTicker, 'TR.CommonName',raw_output=True)
    return df['data'][0][1]

def get_52_week_high_low(iEikonTicker):
    df = ek.get_data(iEikonTicker, ['TR.Price52WeekHigh','TR.Price52WeekHigh'], raw_output=True)
    a52WeekHigh = df['data'][0][1]
    a52WeekLow = df['data'][0][2]
    return [a52WeekHigh,a52WeekLow]

def get_betas(iEikonTicker):
    df = ek.get_data(iEikonTicker,
                     ['TR.BetaWkly3Y',
                      'TR.BetaWklyUp3Y',
                      'TR.BetaWklyDown3Y',
                      'TR.BetaWkly2Y',
                      'TR.BetaWklyUp2Y',
                      'TR.BetaWklyDown2Y'],
                     raw_output=True)
    aBetaWkly3Y=df['data'][0][1]
    aBetaWklyUp3Y=df['data'][0][2]
    aBetaWklyDown3Y=df['data'][0][3]
    aBetaWkly2Y=df['data'][0][4]
    aBetaWklyUp2Y=df['data'][0][5]
    aBetaWklyDown2Y=df['data'][0][6]
    return [aBetaWkly3Y,aBetaWklyUp3Y,aBetaWklyDown3Y,aBetaWkly2Y,aBetaWklyUp2Y,aBetaWklyDown2Y]

def get_daily_updates(iEikonTicker):
    df = ek.get_data(iEikonTicker, ['TR.CompanyMarketCap','TR.EV','CF_LAST','TR.Volume'], raw_output=True)
    aCompanyMktCap = df['data'][0][1]
    aCompanyEV = df['data'][0][2]
    aSharePrice = df['data'][0][3]
    aDailyVolume = df['data'][0][4]
    return [aCompanyMktCap,aCompanyEV,aSharePrice,aDailyVolume]

def get_minority_interest(iEikonTicker):
    df = ek.get_data(iEikonTicker, 'TR.MinorityInterestNonRedeemable', raw_output=True)
    return df['data'][0][1]

def get_fiscal_year_dates(iEikonTicker):
    df = ek.get_data(iEikonTicker,'TR.EBITDA(Period=FY0).periodenddate',raw_output=True)
    month=df['data'][0][1].split("-")[1].lstrip("0")
    aMonthName=calendar.month_abbr[int(month)].upper()
    aMonthDay=df['data'][0][1].split("-")[2]
    return (aMonthDay+'-'+aMonthName)

def get_major_shareholders(iEikonTicker):
    #Init dictionaries
    aMajorOwners={"Top-10-Owners":{}}
    aMajorFunds={"Top-10-Mutual-Funds":{}}
    aTopOwnersDict={}
    aTopFundsDict={}
    iSimplifiedTicker=eikon_to_regular_ticker(iEikonTicker)

    #Request page and parse
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

            #Top Owners names and percentages
            aTopOwnerName=aTopOwner[0].attrib.get('title')
            aTopOwnerPerc=aTopOwnerPerc[0].text_content()
            aPercDict1["Percentage"]=aTopOwnerPerc
            aTopOwnersDict[aTopOwnerName]=aPercDict1

            #Top Funds names and percentages
            aTopaFundName=aFund[0].attrib.get('title')
            aTopFundPerc=aFundPerc[0].text_content()
            aPercDict2["Percentage"]=aTopFundPerc
            aTopFundsDict[aTopaFundName]=aPercDict2

    aMajorOwners["Top-10-Owners"]=aTopOwnersDict
    aMajorFunds["Top-10-Mutual-Funds"]=aTopFundsDict
    return [aMajorOwners,aMajorFunds]


def get_all_eikon_data(iEikonTickers):
    for aEikonTicker in iEikonTickers:
        aBusinessSummary=get_business_summary(aEikonTicker)
        aCompanyName=get_common_name(aEikonTicker)
        a52WeekHighLow=get_52_week_high_low(aEikonTicker)
        aListBetas=get_betas(aEikonTicker)
        aListDailyUpdates=get_daily_updates(aEikonTicker)
        aMinInterest=get_minority_interest(aEikonTicker)
        aMajorShareholders=get_major_shareholders(aEikonTicker)
        aFiscalYearEndDate=get_fiscal_year_dates(aEikonTicker)

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
             'NetDebt']

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
                      'TR.NetDebt(Period='+iPeriod+')'],
                     raw_output=True)
    return [aLabels,df]

def retrieve_fiscal_year_data(iEikonTicker):
    aFiscalYears=['FY0','FY-1','FY-2']
    aFYDataDict={}
    oListJson=[]

    #Obtain last reported Fiscal Year date
    df = ek.get_data(iEikonTicker,'TR.EBITDA(Period=FY0).periodenddate',raw_output=True)
    aLastFYEnd=df['data'][0][1].split("-")
    aLastFYEnd=datetime(int(aLastFYEnd[0]),int(aLastFYEnd[1].lstrip("0")),int(aLastFYEnd[2].lstrip("0")))
    aFY0=aLastFYEnd.year

    for fy in aFiscalYears:
        #NOTE:Historic fiscal year price close
        aLabels,df = retrieve_eikon_reports(iEikonTicker, fy)

        #Get array of all data
        aDfLen=len(df['data'][0])-1
        aFYJson={"FY"+str(aFY0):{}}
        for idx in range(0,aDfLen):
            aFYDataDict[aLabels[idx]]=df['data'][0][idx+1]
        aFYJson["FY"+str(aFY0)]=aFYDataDict
        oListJson.append(copy.deepcopy(aFYJson))
        aFY0=aFY0-1
        print('------------')
    print(oListJson)

def retrieve_eikon_estimates(iEikonTicker, iPeriod):
    aLabels=['TotalRevenueMean',
             'EPSSmart',
             'EPSMean',
             'EVMean',
             'DPSMean',
             'DPSSmart',
             'EBITDASmart',
             'EBITDAMean',
             'EBITSmart',
             'EBITMean',
             'EV/EBITDASmart',
             'FwdEV/EBITDASmart',
             'FwdEV/EBITSmart',
             'NetDebtMean']

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
    aFiscalYears=['FY1','FY2','FY3']
    aFYDataDict={}
    oListJson=[]
    #df = ek.get_data(iEikonTicker,'TR.EpsSmartEst(Period=FY1).fperiod',raw_output=True)
    df = ek.get_data(iEikonTicker,'TR.EpsSmartEst(Period=FY1).periodenddate',raw_output=True)
    aLastFYEnd=df['data'][0][1].split("-")
    aLastFYEnd=datetime(int(aLastFYEnd[0]),int(aLastFYEnd[1].lstrip("0")),int(aLastFYEnd[2].lstrip("0")))
    aFY1=aLastFYEnd.year

    for fy in aFiscalYears:
        #NOTE:Historic fiscal year price close
        aLabels,df = retrieve_eikon_estimates(iEikonTicker, fy)

        #Get array of all data
        aDfLen=len(df['data'][0])-1
        aFYJson={"FY"+str(aFY1):{}}
        for idx in range(0,aDfLen):
            aFYDataDict[aLabels[idx]]=df['data'][0][idx+1]
            print(aLabels[idx])
            print(df['data'][0][idx+1])
        aFYJson["FY"+str(aFY1)]=aFYDataDict
        oListJson.append(copy.deepcopy(aFYJson))
        print(oListJson)
        aFY1=aFY1+1
    return oListJson

def retrieve_estimated_fiscal_quarter_data(iEikonTicker):
    aFiscalQuarters=['FQ1','FQ2','FQ3','FQ4']
    aFQDataDict={}
    oListJson=[]
    aFQDataDict["estimated"]=True
    for fq in aFiscalQuarters:
        df = ek.get_data(iEikonTicker,'TR.EpsSmartEst(Period='+fq+').fperiod',raw_output=True)
        aQuarter=df['data'][0][1]
        aLabels,df = retrieve_eikon_estimates(iEikonTicker, fq)
        aDfLen=len(df['data'][0])-1
        aFYJson={str(aQuarter):{}}
        for idx in range(0,aDfLen):
            aFQDataDict[aLabels[idx]]=df['data'][0][idx+1]
        aFYJson[str(aQuarter)]=aFQDataDict
        oListJson.append(copy.deepcopy(aFYJson))
    print(oListJson)
    return oListJson

def retrieve_estimates_year_data(iEikonTicker):
    df = ek.get_data(iEikonTicker,'TR.EBITDAActValue(Period=FY0)',raw_output=True)

def get_last10k_balance_sheet(iMDBModel, iFormType, numberFilingsBack):
    #Prepare request
    APIPoolIndex = 0
    headers = set_last10k_req_headers(APIPoolIndex)
    params = set_last10k_req_params(iFormType,numberFilingsBack)

    #tickers = retrieve_sp_500_tickers()
    dataType = {"Balance Sheet":{}}
    dataYear = {get_last10k_form_name(iFormType):{}}
    errorList = []
    companies = json.load(open('./bookshelf/SP500.json','r'))
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
        #get_last10k_balance_sheet(model,'10-K','0')
        #get_all_eikon_data()
        retrieve_estimated_fiscal_quarter_data('IBM')

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
