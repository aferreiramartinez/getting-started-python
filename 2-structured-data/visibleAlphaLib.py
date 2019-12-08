import requests
import collections, copy

def getAuthToken():
    url = 'https://app.visiblealpha.com/auth/'
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
    payload = {'username': 'emmanuel@convectorcapital.com', 'password':'Madison645'}
    # Authenticate
    response = requests.post(url, json = payload, headers = headers)
    # if response.status_code == requests.codes.ok:
        # print(response.status_code)
    # else:
    #     print(response.status_code)
    # Save the token
    token = response.json()['jwt']
    headers['Authorization'] = 'Bearer '+token
    return headers


def getLongDataTypeName(iDataType):
    if iDataType == 'BS':
        return 'BalanceSheet'
    elif iDataType == 'IS':
        return 'IncomeStatement'
    elif iDataType == 'CF':
        return 'CashFlow'
    elif iDataType == 'RT':
        return 'Other'

# GET METADATA TO FIND ID'S
def getFinancialsIDs(iTicker, iAuthToken):
    url = 'https://app.visiblealpha.com/api2/'
    name = 'getmeta?&'
    cut = 'cut=SD&'
    ticker = 'ticker='+str(iTicker).split('.')[0]+'&'
    response = requests.get(url+name+cut+ticker,headers=iAuthToken)
    if response.json()['status']=='error':
        ticker = 'ticker='+str(iTicker).split('.')[0]+'_US'+'&'
        response = requests.get(url+name+cut+ticker,headers=iAuthToken)
    params = response.json()['parameter']
    # params = mockGetMeta['parameter']
    financials={'BalanceSheet':{},'IncomeStatement':{},'CashFlow':{},'Other':{}}
    for param in params:
        lineItemName=param['name'].title().replace(",","").replace("'","").replace(" ","").replace("-","").replace("&","And")
        lineItemDoc={}
        lineItemDoc['id']=param['id']
        if param['shortName'] == 'BS':
            financials['BalanceSheet'][lineItemName]=lineItemDoc
        elif param['shortName'] == 'IS':
            financials['IncomeStatement'][lineItemName]=lineItemDoc
        elif param['shortName'] == 'CF':
            financials['CashFlow'][lineItemName]=lineItemDoc
        elif param['shortName'] == 'RT':
            financials['Other'][lineItemName]=lineItemDoc
    return financials

# 'BalanceSheet': {
# 	'PropertyPlantAndEquipmentNet': {
# 		'id': '1217'
# 	},...
# }


# GET BULK DATA WITH getpg
def getBulkForTicker(iTicker, iAuthToken):
    financials = getFinancialsIDs(iTicker,iAuthToken)
    url = 'https://app.visiblealpha.com/api2/'
    name = 'getpg?&'
    cut = 'cut=SD&'
    regularTicker = str(iTicker).split('.')[0]
    ticker = 'ticker='+regularTicker+'&'
    period ='period=FY-2016&period=FY-2017&period=FY-2018&period=FY-2019&period=FY-2020&period=FY-2021&period=FY-2022&period=FY-2023&period=FY-2024&'
    source = 'source=consensus&'
    revision = 'revision=current'
    allDataDoc= collections.defaultdict(dict)
    allDataDoc['EikonTicker']=str(iTicker)
    allDataDoc['Ticker']=str(regularTicker)
    for page in ['pg=BS&','pg=IS&','pg=CF&','pg=RT&']:
        dataType = page.replace("pg=","").replace("&","")
        longDataTypeName = getLongDataTypeName(dataType)
        financeKeys = financials[longDataTypeName].keys()
        response = requests.get(url+name+page+cut+ticker+period+source+revision, headers=iAuthToken)
        if response.json()['status']=='error':
            ticker = 'ticker='+str(iTicker).split('.')[0]+'_US'+'&'
            response = requests.get(url+name+page+cut+ticker+period+source+revision, headers=iAuthToken)
        bulkDataYears = response.json()['data']
        # bulkDataYears = mockBulk['data']
        for singleDataYear in bulkDataYears:
            aFY=singleDataYear['fiscalyear'].replace("-","")
            allDataDoc[aFY]["Estimated"]=singleDataYear['forecast']
            # for every line item id check whether its present and add the value to the financials document
            for financeKey in financeKeys:
                itemId = financials[longDataTypeName][financeKey]['id']
                for lineItem in singleDataYear['data']:
                    if lineItem['param_id'] == itemId:
                        if longDataTypeName not in allDataDoc[aFY]:
                            allDataDoc[aFY][longDataTypeName]={}
                        if financeKey not in allDataDoc[aFY][longDataTypeName]:
                            allDataDoc[aFY][longDataTypeName][financeKey]={}
                        allDataDoc[aFY][longDataTypeName][financeKey]['value'] = copy.deepcopy(lineItem['value'])
                        allDataDoc[aFY][longDataTypeName][financeKey]['id'] = copy.deepcopy(lineItem['param_id'])
    return dict(allDataDoc)



# Create a function called "chunks" with two arguments, l and n:
def chunks(l, n):
    # For item i in a range that is a length of l,
    for i in range(0, len(l), n):
        # Create an index range for l of n items:
        yield l[i:i+n]

# CHUNK VALUES IF WE WANT TO RETRIEVE INDIVIDUAL DATA POINTS
# BSkeys = list(financials['BalanceSheet'].keys())
# # BSvalues =      
# chunkedBSkeys = list(chunks(BSkeys, 10))
# for chunk in chunkedBSkeys:
#     params = "getparam?"
#     for param in chunk:
#         params += 'param='+str(param)+'&'
#     print(params)
#     url2 = 'https://app.visiblealpha.com/api2/'
#     cut = 'cut=SD&'
#     ticker = 'ticker=NFLX&'
#     period ='period=FY-2018&period=FY-2019&period=FY-2020&period=FY-2021&period=FY-2022&&period=FY-2023&'
#     source = 'source=consensus&'
#     revision = 'revision=current'
#     response = requests.get(url2+params+period+cut+ticker+source+revision,headers=headers)
#     item = response.json()
#     print(item)



# print(json_normalize(metadata, 'PG'))
# print(json_normalize(metadata, 'parameter'))
# print(json_normalize(metadata, 'periods'))
# print(json_normalize(metadata, 'sources'))



# GET param data with getpg
# url2 = 'https://app.visiblealpha.com/api2/'
# param= 'getparam?param=1210&param=190&param=198&param=191&param=192&param=193&param=194&param=195&param=196&param=197&param=220&param=208&param=2062&param=203&'
# cut = 'cut=SD&'
# ticker = 'ticker=NFLX&'
# period ='period=FY-2018&period=FY-2019&period=FY-2020&period=FY-2021&period=FY-2022&'
# source = 'source=consensus&'
# revision = 'revision=current'
# response = requests.get(url2+param+period+cut+ticker+source+revision,headers=headers)
# item = response.json()