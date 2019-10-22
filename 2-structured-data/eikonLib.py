import calendar, collections, copy, datetime, math, time, numpy
from datetime import datetime
import eikon as ek

def FloatOrZero(value):
    try:
        if math.isnan(value):
            return 0.0
        else:
            return float(value)
    except:
        return 0.0

def get_business_summary(iEikonTicker):
    aBusinessSummaryJson={}
    aStartIndex=1
    aLabels=['CompanyDescription',
             'HQCountryCode',
             'Currency',
             'GICSSector',
             'GICSIndustryGroup',
             'GICSIndustry',
             'GICSSubIndustry',
             'BusinessSectorScheme',
             'BusinessSector',
             'TradedInIdentifier',
             'MemberIndexRic',
             'PriceMainIndexRIC',
             'SPRating']
    df = ek.get_data(iEikonTicker,
                     ['TR.BusinessSummary',
                      'TR.HQCountryCode',
                      'TR.TotalAssetsReported(Period=FY0).currency',
                      'TR.GICSSector',
                      'TR.GICSIndustryGroup',
                      'TR.GICSIndustry',
                      'TR.GICSSubIndustry',
                      'TR.BusinessSectorScheme',
                      'TR.BusinessSector',
                      'CF_EXCHNG',
                      'TR.MemberIndexRic',
                      'TR.PriceMainIndexRIC',
                      'TR.IssuerRating(IssuerRatingSrc=SPI)'],
                     raw_output=True)
    for business in aLabels:
        aBusinessSummaryJson[business]=str(df['data'][0][aStartIndex])
        aStartIndex+=1
    return aBusinessSummaryJson

def get_common_name(iEikonTicker):
    oNameJson={}
    df = ek.get_data(iEikonTicker, 'TR.CommonName',raw_output=True)
    oNameJson["CompanyName"]=df['data'][0][1]
    return oNameJson

def get_52_week_high_low(iEikonTicker):
    o52WHighLowJson={}
    df = ek.get_data(iEikonTicker, ['TR.Price52WeekHigh','TR.Price52WeekLow'], raw_output=True)
    o52WHighLowJson["Price52WeekHigh"]=FloatOrZero(df['data'][0][1])
    o52WHighLowJson["Price52WeekLow"]=FloatOrZero(df['data'][0][2])
    return o52WHighLowJson

def get_betas(iEikonTicker):
    print("betas")
    oBetasJson={"Betas":{}}
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
        oBetasJson["Betas"][betas]=FloatOrZero(df['data'][0][aStartIndex])
        aStartIndex+=1
    return oBetasJson

def get_30_day_volume(iEikonTicker):
    oAccumulatedVol = 0
    volumes=ek.get_data(iEikonTicker, 'TR.ACCUMULATEDVOLUME(SDate=0,EDate=-29,Frq=D)',raw_output=True)
    for vol in volumes['data']:
        oAccumulatedVol += FloatOrZero(vol[1])
    return oAccumulatedVol

def get_365_day_share_price(iEikonTicker):
    print("365 daily price")
    o365DayPrice={"365DaySharePrice":{}}
    aJson={}
    aPrices=ek.get_data(iEikonTicker, ['TR.PriceClose(SDate=0,EDate=-364,Frq=D)',
                                       'TR.PriceOpen(SDate=0,EDate=-364,Frq=D)',
                                       'TR.PriceHigh(SDate=0,EDate=-364,Frq=D)',
                                       'TR.PriceLow(SDate=0,EDate=-364,Frq=D)',
                                       'TR.PriceClose(SDate=0,EDate=-364,Frq=D).calcdate'],raw_output=True)
    for aPrice in aPrices['data']:
        aJson["PriceClose"]=FloatOrZero(aPrice[1])
        aJson["PriceOpen"]=FloatOrZero(aPrice[2])
        aJson["PriceHigh"]=FloatOrZero(aPrice[3])
        aJson["PriceLow"]=FloatOrZero(aPrice[4])
        o365DayPrice["365DaySharePrice"][str(aPrice[5])]=copy.deepcopy(aJson)
    return o365DayPrice

def get_120_month_share_price(iEikonTicker):
    print("monthly updates")
    o120MonthPrice={"120MonthSharePrice":{}}
    aJson={}
    aPrices=ek.get_data(iEikonTicker,['TR.PriceClose(SDate=0,EDate=-119,Frq=CM)','TR.PriceClose(SDate=0,EDate=-119,Frq=CM).calcdate'],raw_output=True)

    for aPrice in aPrices['data']:
        aJson["PriceClose"]=FloatOrZero(aPrice[1])
        o120MonthPrice["120MonthSharePrice"][str(aPrice[2][:-3])]=copy.deepcopy(aJson)
    return o120MonthPrice

def get_daily_updates(iEikonTicker):
    oDailyJson={"DailyUpdated":{}}
    aStartIndex=1
    aLabels=['CompanyMarketCap',
             'EV',
             'SharePrice',
             'DailyVolume',
             'PE',
             'SharesOutstanding']
    df = ek.get_data(iEikonTicker, ['TR.CompanyMarketCap','TR.EV','CF_LAST','TR.Volume','TR.PE','TR.SharesOutstanding'], raw_output=True)
    for data in aLabels:
        oDailyJson["DailyUpdated"][data] = FloatOrZero(df['data'][0][aStartIndex])
        aStartIndex+=1
    oDailyJson["DailyUpdated"]["30DayVolume"]=get_30_day_volume(iEikonTicker)
    return oDailyJson

def get_competitors(iEikonTicker):
    # ISIN (or CUSIP) represent a financial instrument, whereas RICs represent a financial instrument on a specific market
    print("competitors")
    aIndex=1
    oDailyJson={"Competitors":[]}
    aJson={}
    screener_exp = "SCREEN(U(IN(Peers('"+iEikonTicker+"'))))"
    peers = ek.get_data(instruments=[screener_exp], fields=['TR.CompanyName','TR.RICCode'],raw_output=True)
    for company in peers["data"]:
        aJson["Name"]=company[1]
        aJson["EikonTicker"]=company[2]
        oDailyJson["Competitors"].append(copy.deepcopy(aJson))
        aIndex += 1
    return oDailyJson

#We need:
#TR.FiMoodysRating
#TR.FiSPRating

def get_bonds(iEikonTicker):
    print("bonds")
    aLabels=['BetaWkly3Y',
             'BetaWklyUp3Y',
             'BetaWklyDown3Y',
             'BetaWkly2Y',
             'BetaWklyUp2Y',
             'BetaWklyDown2Y']
    isins=[]
    isinListRaw = ek.get_data(iEikonTicker,'TR.BondISIN',raw_output=True)
    for isin in isinListRaw["data"]:
        isins.append(isin[1])
    df = ek.get_data(isins,['TR.CouponRate','TR.FiMaturityDate','Tr.FiAssetTypeDescription','TR.FiMaturityStandardYield'],raw_output=True)
    print ()

def get_minority_interest(iEikonTicker):
    print("min interest")
    oMinInterest={}
    df = ek.get_data(iEikonTicker, 'TR.MinorityInterestNonRedeemable', raw_output=True)
    oMinInterest["MinorityInterest"]=FloatOrZero(df['data'][0][1])
    return oMinInterest

def get_fiscal_year_dates(iEikonTicker):
    print("FY dates")
    oFY={}
    df = ek.get_data(iEikonTicker,'TR.EBITDA(Period=FY0).periodenddate',raw_output=True)
    # 'data': [['SJM.N', '2019-04-30']]
    fulldate=df['data'][0][1].split("-")
    if fulldate == ['']:
        df = ek.get_data(iEikonTicker,'TR.Revenue(Period=FY0).periodenddate',raw_output=True)
        fulldate=df['data'][0][1].split("-")
    if fulldate == ['']:
        print('ERROR NO FISCAL YEAR END DATE USING EBITDA OR REVENUE')
    month = fulldate[1].lstrip("0")
    aMonthName=calendar.month_abbr[int(month)].upper()
    aMonthDay=df['data'][0][1].split("-")[2]
    oFY["FYEndDate"]=(aMonthDay+'-'+aMonthName)
    return oFY

def get_major_shareholders(iEikonTicker):
    maxShareholders = 10
    oMajorOwners = {"Top-10-Owners":[]}
    aJson={}
    aHolders = ek.get_data(instruments=[iEikonTicker], fields=['TR.InvestorFullName',
                                                               'TR.SharesHeld',
                                                               'TR.PctOfSharesOutHeld'],raw_output=True)
    for aHolder in aHolders["data"]:
        if maxShareholders >= 0:
            maxShareholders -= 1
            aJson["Name"]=aHolder[1].replace(".","")
            aJson["Percentage"]=FloatOrZero(aHolder[3])
            aJson["SharesOutstandingHeld"]=FloatOrZero(aHolder[2])

            oMajorOwners["Top-10-Owners"].append(copy.deepcopy(aJson))
        else:
            break
    return oMajorOwners

#def get_fiscal_quarter_end_date(iEikonTicker, iPeriod, iNumPeriods, iEstimated):
#    if iEstimated is False:
#        aPeriod,err = ek.get_data(iEikonTicker,
#                              ['TR.EBITDA.fperiod','TR.EBITDA.periodenddate'],
#                              {'SDate':'0','EDate':'-'+iNumPeriods,'FRQ':iPeriod,'Period':iPeriod+'0'})
#        aQuarters=aPeriod.iloc[:,1:2].values.flatten().tolist()
#        aQuarterEndDates=aPeriod.iloc[:,2:3].values.flatten().tolist()
#        if all(aQuarters) is False:
#            aPeriod,err = ek.get_data(iEikonTicker,
#                                  ['TR.EBITDAActValue.fperiod', 'TR.EBITDAActValue.periodenddate'],
#                                  {'SDate':'0','EDate':'-'+iNumPeriods,'FRQ':iPeriod,'Period':iPeriod+'0'})
#            aQuarters=aPeriod.iloc[:,1:2].values.flatten().tolist()
#            aQuarterEndDates=aPeriod.iloc[:,2:3].values.flatten().tolist()
#    else:
#        aPeriod,err = ek.get_data(iEikonTicker,
#                              ['TR.EPSMean.fperiod','TR.EPSMean.periodenddate'],
#                              {'SDate':'0','EDate':iNumPeriods,'FRQ':iPeriod,'Period':iPeriod+'1'})
#        aQuarters=aPeriod.iloc[:,1:2].values.flatten().tolist()
#        aQuarterEndDates=aPeriod.iloc[:,2:3].values.flatten().tolist()
#        if all(aQuarters) is False:
#            aPeriod,err = ek.get_data(iEikonTicker,
#                                  ['TR.EpsSmartEst.fperiod','TR.EpsSmartEst.periodenddate'],
#                                  {'SDate':'0','EDate':iNumPeriods,'FRQ':iPeriod,'Period':iPeriod+'1'})
#            aQuarters=aPeriod.iloc[:,1:2].values.flatten().tolist()
#            aQuarterEndDates=aPeriod.iloc[:,2:3].values.flatten().tolist()
#    print(aQuarters)
#    print(aQuarterEndDates)
#    return [aQuarters,aQuarterEndDates]

def get_all_year_data(iEikonTicker):
    oJson={"DataByFiscalYear":{}}
    aFiscalYearData=retrieve_fiscal_year_data(iEikonTicker)
    aFiscalYearEstimatesData=retrieve_estimated_fiscal_year_data(iEikonTicker)
    oJson["DataByFiscalYear"]=aFiscalYearData
    oJson["DataByFiscalYear"].update(aFiscalYearEstimatesData)
    return oJson

def retrieve_eikon_reports(iEikonTicker, iPeriod , iNumPeriods):
    aLabels=collections.OrderedDict({'CashAndEquiv':'BalanceSheet',
                                     'TotalCurrentAssets':'BalanceSheet',
                                     'TotalCurrentLiabilities':'BalanceSheet',
                                     'CurrentDeferredRevenue':'BalanceSheet',
                                     'LTDeferredRevenue':'BalanceSheet',
                                     'NetTradeAccReceivable':'BalanceSheet',
                                     'Inventories':'BalanceSheet',
                                     'IntangiblesNet':'BalanceSheet',
                                     'NetPropertyPlantEquipment':'BalanceSheet',
                                     'GoodwillNet':'BalanceSheet',
                                     'LTInvestments':'BalanceSheet',
                                     'OtherCurrentAssets':'BalanceSheet',
                                     'TotalAssets':'BalanceSheet',
                                     'TradeAccPayable':'BalanceSheet',
                                     'TotalLiabilities':'BalanceSheet',
                                     'TotalLTDebt':'BalanceSheet',
                                     'CurrentLTDebt':'BalanceSheet',
                                     'TotalSTBorrowings':'BalanceSheet',
                                     'NotesPayableSTDebt':'BalanceSheet',
                                     'PreferredStock':'BalanceSheet',
                                     'TotalLiabAndShareholdersEquity':'BalanceSheet',
                                     'CashAndSTInv':'BalanceSheet',
                                     'TotalDebt':'BalanceSheet',
                                     'NetDebt':'BalanceSheet',
                                     'MinorityInterest':'BalanceSheet',
                                     'DilSharesOut':'Other',
                                     'EV/EBITDA':'Other',
                                     'EV/EBIT':'Other',
                                     'EV':'Other',
                                     'P/E':'Other',
                                     'BookValuePerShare':'Other',
                                     'GrossMarginPct':'Other',
                                     'EBITMarginPct':'Other',
                                     'EBITDAMarginPct':'Other',
                                     'DPS':'IncomeStatement',
                                     'EBITDA':'IncomeStatement',
                                     'EBIT':'IncomeStatement',
                                     'TotalRevenue':'IncomeStatement',
                                     'EPS':'IncomeStatement',
                                     'EPSInclExtra':'IncomeStatement',
                                     'GrossProfit':'IncomeStatement',
                                     'NetIncome':'IncomeStatement',
                                     'NetIncomeBeforeExtra':'IncomeStatement',
                                     'ProvisionForIncomeTaxes':'IncomeStatement',
                                     'DiscOperations':'IncomeStatement',
                                     'FreeCashFlow':'CashFlow',
                                     'Capex':'CashFlow',
                                     'CashIntPaid':'CashFlow',
                                     'CashTaxesPaid':'CashFlow',
                                     'ChangesInWorkingCapital':'CashFlow',
                                     'AcqOfBusiness':'CashFlow',
                                     'SaleOfBusiness':'CashFlow',
                                     'PurchaseOfInvest':'CashFlow',
                                     'LTDebtReduction':'CashFlow',
                                     'STDebtReduction':'CashFlow',
                                     'Proceeds':'CashFlow',
                                     'RepurchaseOfStock':'CashFlow',
                                     'CashDividendsPaid':'CashFlow',
                                     'NetCashEndingBalance':'CashFlow',
                                     'NetCashBeginningBalance':'CashFlow',
                                     'CashFromOperatingActivities':'CashFlow',
                                     'CashFromInvestingActivities':'CashFlow',
                                     'CashFromFinancingActivities':'CashFlow'})
    oLabels=list(aLabels.items())
    df = ek.get_data(iEikonTicker,
                     ['TR.CashandEquivalents',
                      'TR.CurrentAssetsActValue',
                      'TR.CurrentLiabilitiesActValue',
                      'TR.DeferredRevenueActValue',
                      'TR.LTDefRevActValue',
                      'TR.AcctsReceivTradeNet',
                      'TR.TotalInventory',
                      'TR.IntangiblesNet',
                      'TR.PropertyPlantEquipmentTotalNet',
                      'TR.GoodwillNet',
                      'TR.LTInvestments',
                      'TR.OtherCurrentAssets',
                      'TR.TotalAssetsReported',
                      'TR.AccountsPayable',
                      'TR.TotalLiabilities',
                      'TR.TotalLongTermDebt',
                      'TR.CurrentPortionLTDebtToCapitalLeases',
                      'TR.TotalSTBorrowings',
                      'TR.NotesPayableSTDebt',
                      'TR.PreferredStockNonRedeemableNet',
                      'TR.TotalLiabilitiesAndShareholdersEquity',
                      'TR.CashAndSTInvestments',
                      'TR.TotalDebtOutstanding',
                      'TR.NetDebt',
                      'TR.MinorityInterestBSStmt',
                      'TR.DilutedWghtdAvgShares',
                      'TR.HistEnterpriseValueEBITDA',
                      'TR.EVEBIT',
                      'TR.HistEnterpriseValue',
                      'TR.HistPE',
                      'TR.BookValuePerShare',
                      'TR.GrossMargin',
                      'TR.EBITMarginPercent',
                      'TR.EBITDAMarginPercent',
                      'TR.DpsCommonStock',
                      'TR.EBITDAActValue',
                      'TR.EBITActValue',
                      'TR.TotalRevenue',
                      'TR.DilutedEpsExclExtra',
                      'TR.DilutedEpsInclExtra',
                      'TR.GrossProfit',
                      'TR.NetIncome',
                      'TR.NetIncomeBeforeExtraItems',
                      'TR.ProvisionForIncomeTaxes',
                      'TR.DiscontinuedOperations',
                      'TR.FCFActValue',
                      'TR.CapexActValue',
                      'TR.CashInterestPaid',
                      'TR.CashTaxesPaid',
                      'TR.ChangesInWorkingCapital',
                      'TR.AcquisitionOfBusiness',
                      'TR.SaleOfBusiness',
                      'TR.PurchaseOfInvestments',
                      'TR.LTDebtReduction',
                      'TR.STDebtReduction',
                      'TR.SaleMaturityofInvestment',
                      'TR.RepurchaseRetirementOfCommon',
                      'TR.TotalDividendsActValue',
                      'TR.NetCashEndingBalance',
                      'TR.NetCashBeginningBalance',
                      'TR.CashFlowfromOperationsActValue',
                      'TR.CashFlowfromInvestingActValue',
                      'TR.CashFlowfromFinancingActValue',
                      'TR.EBITDA.fperiod',
                      'TR.EBITDA.periodenddate',
                      'TR.EBITDAActValue.fperiod',
                      'TR.EBITDAActValue.periodenddate'],
                     {'SDate':'-'+iNumPeriods,'EDate':'0','FRQ':iPeriod,'Period':iPeriod+'0'},
                     raw_output=True)
    # integrity check for forgotten commas
    # we retrieve 5 items more in df than we have in the oLabels since we have the ticker and 4 dates
    if len(oLabels) is not (len(df['data'][0])-5):
        print('Labels length:' +str(len(oLabels)))
        print('Df length:' +str(len(df['data'][0])-5))
        raise Exception('ERROR: Missing commas in eikon reports')

    return [oLabels,df]

def double_check_FY_data(iTicker,iPeriod,ioJsonData):
    if ioJsonData['IncomeStatement']['EBITDA'] == 0.0:
        df = ek.get_data(iTicker, 'TR.EBITDA(Period='+iPeriod+')',raw_output=True)
        ioJsonData['IncomeStatement']['EBITDA']=FloatOrZero(df['data'][0][1])
    if ioJsonData['IncomeStatement']['EBIT'] == 0.0:
        df = ek.get_data(iTicker, 'TR.EBIT(Period='+iPeriod+')',raw_output=True)
        ioJsonData['IncomeStatement']['EBIT']=FloatOrZero(df['data'][0][1])
    return ioJsonData

def retrieve_fiscal_year_data(iEikonTicker):
    print("retrieve_fiscal_year_data")
    aFiscalYears=['FY-4','FY-3','FY-2','FY-1','FY0']
    numberOfYears = len(aFiscalYears)
    oListJson={}
    aFYDataDict= collections.defaultdict(dict)
    aFYDataDict["Estimated"]="false"

    #Obtain last reported Fiscal Year date
    df = ek.get_data(iEikonTicker,'TR.EBITDA(Period=FY0).periodenddate',raw_output=True) #'data': [['AAPL.O', '2018-09-29']]
    aLastFYEnd=df['data'][0][1].split("-") #['2018', '09', '29']
    if aLastFYEnd == ['']:
        df = ek.get_data(iEikonTicker,'TR.Revenue(Period=FY0).periodenddate',raw_output=True)
        aLastFYEnd=df['data'][0][1].split("-")
    if aLastFYEnd == ['']:
        print('ERROR NO SMART DATE PERIOD END DATE')
    aLastFYEnd=datetime(int(aLastFYEnd[0]),int(aLastFYEnd[1].lstrip("0")),int(aLastFYEnd[2].lstrip("0")))#datetime.datetime(2018, 9, 29, 0, 0)
    aFY0=aLastFYEnd.year-len(aFiscalYears)+1 #2018-5+1=2014

    aLabels,df = retrieve_eikon_reports(iEikonTicker,'FY',str(numberOfYears-1))

    for indx,fy in enumerate(aFiscalYears,start=0): # 0 FY-4, 1 FY-3...
        #NOTE:Historic fiscal year price close
        #Get array of all data, first elem is the ticker which is not needed, last 4 are the period end dates
        aDfLen=len(df['data'][0])-5
        aFYJson={"FY"+str(aFY0):{}}
        for idx in range(0,aDfLen):
            aFYDataDict[aLabels[idx][1]][aLabels[idx][0]]=FloatOrZero(df['data'][indx][idx+1]) #'data': [['AAPL.O', 39510000000, 60503000000], ['AAPL.O', 53394000000, 81730000000]...
        double_check_FY_data(iEikonTicker,fy,aFYDataDict)
        aFYJson["FY"+str(aFY0)]=dict(aFYDataDict)
        oListJson.update(copy.deepcopy(aFYJson))
        aFY0=aFY0+1
    return oListJson

def retrieve_eikon_estimates(iEikonTicker, iPeriod, iNumPeriods):
    aLabels=collections.OrderedDict({'GrossIncomeMean':'IncomeStatement',
                                     'TotalRevenueMean':'IncomeStatement',
                                     'EPSSmart':'IncomeStatement',
                                     'EPSMean':'IncomeStatement',
                                     'DPSMean':'IncomeStatement',
                                     'DPSSmart':'IncomeStatement',
                                     'EBITDASmart':'IncomeStatement',
                                     'EBITDAMean':'IncomeStatement',
                                     'EBITSmart':'IncomeStatement',
                                     'EBITMean':'IncomeStatement',
                                     'ProvisionForIncomeTaxesMean':'IncomeStatement',
                                     'NetIncomeBeforeExtraMean':'IncomeStatement',
                                     'NetIncomeMean':'IncomeStatement',
                                     'TotalDebtMean': 'BalanceSheet',
                                     'NetDebtMean': 'BalanceSheet',
                                     'CashAndEquivalents': 'BalanceSheet',
                                     'CurrentAssets': 'BalanceSheet',
                                     'CurrentDeferredRevenue':'BalanceSheet',
                                     'LTDeferredRevenue':'BalanceSheet',
                                     'CurrentLiabilities':'BalanceSheet',
                                     'GoodwillMean':'BalanceSheet',
                                     'InventoryMean':'BalanceSheet',
                                     'InventorySmartEst':'BalanceSheet',
                                     'ShareholderEquity':'BalanceSheet',
                                     'TotalAssetsMean':'BalanceSheet',
                                     'NetWorkingCapital':'CashFlow',
                                     'CashFromOperatingActivitiesMean':'CashFlow',
                                     'FreeCashFlowMean':'CashFlow',
                                     'IntExpMean':'CashFlow',
                                     'CAPEXMean':'CashFlow',
                                     'DividendsPaid':'CashFlow',
                                     'CashFromFinancingActivitiesMean':'CashFlow',
                                     'CashFromInvestingActivitiesMean':'CashFlow',
                                     'NetWorkingCapMean':'CashFlow',
                                     'EV/EBITDASmart':'Other',
                                     'FwdEV/EBITDASmart':'Other',
                                     'FwdEV/EBITSmart':'Other',
                                     'PESmart':'Other',
                                     'BVPSMean':'Other',
                                     'EVMean': 'Other',
                                     'GrossMarginMean':'Other',
                                     'GrossMarginSmart':'Other'})
    oLabels=list(aLabels.items())
    df = ek.get_data(iEikonTicker,
                     ['TR.GrossIncomeMean',
                      'TR.RevenueMean',
                      'TR.EpsSmartEst',
                      'TR.EPSMean',
                      'TR.DPSMean',
                      'TR.DPSSmartEst',
                      'TR.EBITDASmartEst',
                      'TR.EBITDAMean',
                      'TR.EBITSmartEst',
                      'TR.EBITMean',
                      'TR.TaxProvisionMean',
                      'TR.NetIncomeMean',
                      'TR.RepNetProfitMean',
                      'TR.TotalDebtMean',
                      'TR.NetDebtMean',
                      'TR.Cash&EquivalentsMean',
                      'TR.CurrentAssetsMean',
                      'TR.DeferredRevenueMean',
                      'TR.LTDefRevMean',
                      'TR.CurrentLiabilitiesMean',
                      'TR.GoodwillMean',
                      'TR.InventoryMean',
                      'TR.InventorySmartEst',
                      'TR.ShareholdersEquityMean',
                      'TR.TotalAssetsMean',
                      'TR.NWCMean',
                      'TR.CashFlowfromOperationsMeanEstimate',
                      'TR.FCFMean',
                      'TR.IntExpMean',
                      'TR.CAPEXMean',
                      'TR.TotalDividendsMeanEstimate',
                      'TR.CashFlowfromFinancingMeanEstimate',
                      'TR.CashFlowfromInvestingMeanEstimate',
                      'TR.NWCMean',
                      'TR.EVtoEBITDASmartEst',
                      'TR.FwdEVtoEBTSmartEst',
                      'TR.FwdEVtoEBISmartEst',
                      'TR.FwdPtoEPSSmartEst',
                      'TR.BVPSMean',
                      'TR.EVMean',
                      'TR.GPMMean',
                      'TR.GPMSmartEst',
                      'TR.EPSMean.fperiod',
                      'TR.EPSMean.periodenddate',
                      'TR.EpsSmartEst.fperiod',
                      'TR.EpsSmartEst.periodenddate'],
                     {'SDate':'0','EDate':iNumPeriods,'FRQ':iPeriod,'Period':iPeriod+'1'},
                     raw_output=True)

    # integrity check for forgotten commas
    # we retrieve 5 items more in df than we have in the oLabels since we have the ticker and 4 dates
    if len(oLabels) is not (len(df['data'][0])-5):
        print('Labels length:' +str(len(oLabels)))
        print('Df length:' +str(len(df['data'][0])-5))
        raise Exception('ERROR: Missing commas in estimated eikon reports')
    return [oLabels,df]

def eikon_to_regular_ticker(iEikonTicker):
    regularTicker=iEikonTicker.split('.', 1)[0]
    #Todo check if the second split is needed
    regularTicker=regularTicker.split('.', 1)[0]
    print("regular ticker"+regularTicker)
    return regularTicker

def retrieve_estimated_fiscal_year_data(iEikonTicker):
    print("retrieve_estimated_fiscal_year_data")
    aFiscalYears=['FY1','FY2','FY3','FY4']
    aFYDataDict=collections.defaultdict(dict)
    aFYDataDict["Estimated"]="true"
    oListJson={}
    df = ek.get_data(iEikonTicker,'TR.EPSMean(Period=FY1).periodenddate',raw_output=True)
    aLastFYEnd=df['data'][0][1].split("-")
    if aLastFYEnd == ['']:
        df = ek.get_data(iEikonTicker,'TR.EpsSmartEst(Period=FY1).periodenddate',raw_output=True)
        aLastFYEnd=df['data'][0][1].split("-")
    if aLastFYEnd == ['']:
        df = ek.get_data(iEikonTicker,'TR.EBITDAMean(Period=FY1).periodenddate',raw_output=True)
        aLastFYEnd=df['data'][0][1].split("-")
    if aLastFYEnd == ['']:
        print('ERROR NO SMART DATE PERIOD END DATE')
    aLastFYEnd=datetime(int(aLastFYEnd[0]),int(aLastFYEnd[1].lstrip("0")),int(aLastFYEnd[2].lstrip("0")))
    aFY1=aLastFYEnd.year
    #NOTE:Estimated fiscal year price close
    aLabels,df = retrieve_eikon_estimates(iEikonTicker,'FY',str(len(aFiscalYears)-1))
    for indx,fy in enumerate(aFiscalYears,start=0):
        #Get array of all data, first parameter is ticker, it isnt needed
        aDfLen=len(df['data'][0])-5
        aFYJson={"FY"+str(aFY1):{}}
        for idx in range(0,aDfLen):
            aFYDataDict[aLabels[idx][1]][aLabels[idx][0]]=FloatOrZero(df['data'][indx][idx+1])
        aFYJson["FY"+str(aFY1)]=dict(aFYDataDict)
        oListJson.update(copy.deepcopy(aFYJson))
        aFY1=aFY1+1
    return oListJson

def retrieve_fiscal_quarter_data(iEikonTicker):
    print("quarterly data")
    aNumOfQuarters=16
    aNumOfEstQuarters=4
    aFQDataDict=collections.defaultdict(dict)
    oListJson={"DataByFiscalQuarter":{}}

    #Retrieve data and estimates, we do -1 since {'SDate':'0','EDate':'5'...} will retrieve 6 elems not 5.
    aLabels,df = retrieve_eikon_reports(iEikonTicker, 'FQ',str(aNumOfQuarters-1))
    aEstLabels,estDf = retrieve_eikon_estimates(iEikonTicker, 'FQ',str(aNumOfEstQuarters-1))

    for qtrIdx in range(aNumOfQuarters):
        aFQDataDict.clear()
        aFQDataDict["Estimated"]="false"

        #Get quarter dates and verify validity, if there is no fiscal quarter, do not add empty key
        aDates = df['data'][qtrIdx][-4:]
        aQuarter= aDates[0] if aDates[0] is not '' else aDates[2]
        aQuarterEndDate= aDates[1] if aDates[1] is not '' else aDates[3]
        if aQuarter is '' or aQuarterEndDate is '':
            print(qtrIdx)
            print('WARNING QUARTER MISSING')
            continue

        #First elem of df is always the company name, last 4 are the period end dates we dont need them for data
        aDfLen=len(df['data'][0])-5
        aFYJson={str(aQuarter):{}}
        for idx in range(0,aDfLen):
            aFQDataDict[aLabels[idx][1]][aLabels[idx][0]]=FloatOrZero(df['data'][qtrIdx][idx+1])
        aFYJson[str(aQuarter)]=dict(aFQDataDict)
        aFYJson[str(aQuarter)]["PeriodEndDate"]=aQuarterEndDate
        oListJson["DataByFiscalQuarter"].update(copy.deepcopy(aFYJson))

    for estIdx in range(aNumOfEstQuarters):
        aFQDataDict.clear()
        aFQDataDict["Estimated"]="true"

        #Get quarter dates and verify validity, if there is no fiscal quarter, do not add empty key
        aDates = estDf['data'][estIdx][-4:]
        aQuarter= aDates[0] if aDates[0] is not '' else aDates[2]
        aQuarterEndDate= aDates[1] if aDates[1] is not '' else aDates[3]
        if aQuarter is '' or aQuarterEndDate is '':
            print(estIdx)
            print('WARNING QUARTER MISSING')
            continue

        #First elem of df is always the company name, last 4 are the period end dates we dont need them for data
        aDfLen=len(estDf['data'][0])-5
        aFYJson={str(aQuarter):{}}
        for idx in range(0,aDfLen):
            aFQDataDict[aEstLabels[idx][1]][aEstLabels[idx][0]]=FloatOrZero(estDf['data'][estIdx][idx+1])
        aFYJson[str(aQuarter)]=dict(aFQDataDict)
        aFYJson[str(aQuarter)]["PeriodEndDate"]=aQuarterEndDate
        oListJson["DataByFiscalQuarter"].update(copy.deepcopy(aFYJson))
    return oListJson
