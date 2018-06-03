# operations https://services.last10k.com/v1/company/{ticker}/operations?formType=10-K&filingOrder=0
# liabilities https://services.last10k.com/v1/company/{ticker}/liabilities?formType=10-K&filingOrder=0
# stock-quote https://services.last10k.com/v1/company/VIAB/quote
# conn.request("GET", "/v1/company/latestfilings?%s" % params, "{body}", headers) https://services.last10k.com/v1/company/latestfilings[?formType]


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