import calendar, collections, copy, datetime, math, time
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
             'MoodysRating']
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
                      'TR.IssuerRating'],
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
                                       'TR.PriceHigh(SDate=0,EDate=-364,Frq=D)',
                                       'TR.PriceLow(SDate=0,EDate=-364,Frq=D)',
                                       'TR.PriceClose(SDate=0,EDate=-364,Frq=D).calcdate'],raw_output=True)
    for aPrice in aPrices['data']:
        aJson["PriceClose"]=FloatOrZero(aPrice[1])
        aJson["PriceHigh"]=FloatOrZero(aPrice[2])
        aJson["PriceLow"]=FloatOrZero(aPrice[3])
        o365DayPrice["365DaySharePrice"][str(aPrice[4])]=copy.deepcopy(aJson)
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
             'PE']
    df = ek.get_data(iEikonTicker, ['TR.CompanyMarketCap','TR.EV','CF_LAST','TR.Volume','TR.PE'], raw_output=True)
    for data in aLabels:
        oDailyJson["DailyUpdated"][data] = FloatOrZero(df['data'][0][aStartIndex])
        aStartIndex+=1
    oDailyJson["DailyUpdated"]["30DayVolume"]=get_30_day_volume(iEikonTicker)
    return oDailyJson

def get_competitors(iEikonTicker):
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
    month=df['data'][0][1].split("-")[1].lstrip("0")
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

def get_fiscal_quarter_end_date(iEikonTicker, iPeriod, iEstimated):
    if iEstimated is False:
        aPeriod = ek.get_data(iEikonTicker,['TR.EBITDA(Period='+iPeriod+').fperiod','TR.EBITDA(Period='+iPeriod+').periodenddate'],raw_output=True)
        aQuarter=aPeriod['data'][0][1]
        aQuarterEndDate=aPeriod['data'][0][2]
        if aQuarter == '':
            aPeriod = ek.get_data(iEikonTicker,['TR.EBITDAActValue(Period='+iPeriod+').fperiod', 'TR.EBITDAActValue(Period='+iPeriod+').periodenddate'],raw_output=True)
            aQuarter=aPeriod['data'][0][1]
            aQuarterEndDate=aPeriod['data'][0][2]
    else:
        aPeriod = ek.get_data(iEikonTicker,['TR.EPSMean(Period='+iPeriod+').fperiod','TR.EPSMean(Period='+iPeriod+').periodenddate'],raw_output=True)
        aQuarter=aPeriod['data'][0][1]
        aQuarterEndDate=aPeriod['data'][0][2]
        if aQuarter == ['']:
            aPeriod = ek.get_data(iEikonTicker,['TR.EpsSmartEst(Period='+iPeriod+').fperiod','TR.EpsSmartEst(Period='+iPeriod+').periodenddate'],raw_output=True)
            aQuarter=aPeriod['data'][0][1]
            aQuarterEndDate=aPeriod['data'][0][2]
    return [aQuarter,aQuarterEndDate]

def get_all_year_data(iEikonTicker):
    oJson={"DataByFiscalYear":{}}
    aFiscalYearData=retrieve_fiscal_year_data(iEikonTicker)
    aFiscalYearEstimatesData=retrieve_estimated_fiscal_year_data(iEikonTicker)
    oJson["DataByFiscalYear"]=aFiscalYearData
    oJson["DataByFiscalYear"].update(aFiscalYearEstimatesData)
    return oJson

def retrieve_eikon_reports(iEikonTicker, iPeriod):
    aLabels=collections.OrderedDict({'CashAndEquiv':'BalanceSheet',
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
                     ['TR.CashandEquivalents(Period='+iPeriod+')',
                      'TR.AcctsReceivTradeNet(Period='+iPeriod+')',
                      'TR.TotalInventory(Period='+iPeriod+')',
                      'TR.IntangiblesNet(Period='+iPeriod+')',
                      'TR.PropertyPlantEquipmentTotalNet(Period='+iPeriod+')',
                      'TR.GoodwillNet(Period='+iPeriod+')',
                      'TR.LTInvestments(Period='+iPeriod+')',
                      'TR.OtherCurrentAssets(Period='+iPeriod+')',
                      'TR.TotalAssetsReported(Period='+iPeriod+')',
                      'TR.AccountsPayable(Period='+iPeriod+')',
                      'TR.TotalLiabilities(Period='+iPeriod+')',
                      'TR.TotalLongTermDebt(Period='+iPeriod+')',
                      'TR.CurrentPortionLTDebtToCapitalLeases(Period='+iPeriod+')',
                      'TR.TotalSTBorrowings(Period='+iPeriod+')',
                      'TR.NotesPayableSTDebt(Period='+iPeriod+')',
                      'TR.PreferredStockNonRedeemableNet(Period='+iPeriod+')',
                      'TR.TotalLiabilitiesAndShareholdersEquity(Period='+iPeriod+')',
                      'TR.CashAndSTInvestments(Period='+iPeriod+')',
                      'TR.TotalDebtOutstanding(Period='+iPeriod+')',
                      'TR.NetDebt(Period='+iPeriod+')',
                      'TR.MinorityInterestBSStmt(Period='+iPeriod+')',
                      'TR.DilutedWghtdAvgShares(Period='+iPeriod+')',
                      'TR.HistEnterpriseValueEBITDA(Period='+iPeriod+')',
                      'TR.EVEBIT(Period='+iPeriod+')',
                      'TR.HistEnterpriseValue(Period='+iPeriod+')',
                      'TR.HistPE(Period='+iPeriod+')',
                      'TR.GrossMargin(Period='+iPeriod+')',
                      'TR.EBITMarginPercent(Period='+iPeriod+')',
                      'TR.EBITDAMarginPercent(Period='+iPeriod+')',
                      'TR.DpsCommonStock(Period='+iPeriod+')',
                      'TR.EBITDAActValue(Period='+iPeriod+')',
                      'TR.EBITActValue(Period='+iPeriod+')',
                      'TR.TotalRevenue(Period='+iPeriod+')',
                      'TR.DilutedEpsExclExtra(Period='+iPeriod+')',
                      'TR.DilutedEpsInclExtra(Period='+iPeriod+')',
                      'TR.GrossProfit(Period='+iPeriod+')',
                      'TR.NetIncome(Period='+iPeriod+')',
                      'TR.NetIncomeBeforeExtraItems(Period='+iPeriod+')',
                      'TR.ProvisionForIncomeTaxes(Period='+iPeriod+')',
                      'TR.DiscontinuedOperations(Period='+iPeriod+')',
                      'TR.FreeCashFlow(Period='+iPeriod+')',
                      'TR.CapitalExpenditures(Period='+iPeriod+')',
                      'TR.CashInterestPaid(Period='+iPeriod+')',
                      'TR.CashTaxesPaid(Period='+iPeriod+')',
                      'TR.ChangesInWorkingCapital(Period='+iPeriod+')',
                      'TR.AcquisitionOfBusiness(Period='+iPeriod+')',
                      'TR.SaleOfBusiness(Period='+iPeriod+')',
                      'TR.PurchaseOfInvestments(Period='+iPeriod+')',
                      'TR.LTDebtReduction(Period='+iPeriod+')',
                      'TR.STDebtReduction(Period='+iPeriod+')',
                      'TR.SaleMaturityofInvestment(Period='+iPeriod+')',
                      'TR.RepurchaseRetirementOfCommon(Period='+iPeriod+')',
                      'TR.TotalCashDividendsPaid(Period='+iPeriod+')',
                      'TR.NetCashEndingBalance(Period='+iPeriod+')',
                      'TR.NetCashBeginningBalance(Period='+iPeriod+')',
                      'TR.CashFromOperatingActivities(Period='+iPeriod+')',
                      'TR.CashFromInvestingActivities(Period='+iPeriod+')',
                      'TR.CashFromFinancingActivities(Period='+iPeriod+')'],
                     raw_output=True)
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
    aFiscalYears=['FY-3','FY-2','FY-1','FY0']
    aFYDataDict= collections.defaultdict(dict)
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
            aFYDataDict[aLabels[idx][1]][aLabels[idx][0]]=FloatOrZero(df['data'][0][idx+1])
        double_check_FY_data(iEikonTicker,fy,aFYDataDict)
        aFYJson["FY"+str(aFY0)]=dict(aFYDataDict)
        oListJson.update(copy.deepcopy(aFYJson))
        aFY0=aFY0+1
    return oListJson

def retrieve_eikon_estimates(iEikonTicker, iPeriod):
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
                                     'ProvisionForIncomeTaxes':'IncomeStatement',
                                     'NetIncomeBeforeExtra':'IncomeStatement',
                                     'NetIncome':'IncomeStatement',
                                     'TotalDebtMean': 'BalanceSheet',
                                     'NetDebtMean': 'BalanceSheet',
                                     'CashAndEquivalents': 'BalanceSheet',
                                     'CurrentAssets': 'BalanceSheet',
                                     'DeferredRevenue':'BalanceSheet',
                                     'CurrentLiabilities':'BalanceSheet',
                                     'GoodwillMean':'BalanceSheet',
                                     'InventoryMean':'BalanceSheet',
                                     'InventorySmartEst':'BalanceSheet',
                                     'ShareholderEquity':'BalanceSheet',
                                     'TotalAssets':'BalanceSheet',
                                     'CashFromOperatingActivities':'CashFlow',
                                     'FreeCashFlowMean':'CashFlow',
                                     'IntExpMean':'CashFlow',
                                     'CAPEXMean':'CashFlow',
                                     'DividendsPaid':'CashFlow',
                                     'CashFromFinancingActivities':'CashFlow',
                                     'CashFromInvestingActivities':'CashFlow',
                                     'NetWorkingCapMean':'CashFlow',
                                     'EV/EBITDASmart':'Other',
                                     'FwdEV/EBITDASmart':'Other',
                                     'FwdEV/EBITSmart':'Other',
                                     'PESmart':'Other',
                                     'EVMean': 'Other',
                                     'GrossMarginMean':'Other',
                                     'GrossMarginSmart':'Other'})
    oLabels=list(aLabels.items())
    df = ek.get_data(iEikonTicker,
                     ['TR.GrossIncomeMean(Period='+iPeriod+')',
                      'TR.RevenueMean(Period='+iPeriod+')',
                      'TR.EpsSmartEst(Period='+iPeriod+')',
                      'TR.EPSMean(Period='+iPeriod+')',
                      'TR.DPSMean(Period='+iPeriod+')',
                      'TR.DPSSmartEst(Period='+iPeriod+')',
                      'TR.EBITDASmartEst(Period='+iPeriod+')',
                      'TR.EBITDAMean(Period='+iPeriod+')',
                      'TR.EBITSmartEst(Period='+iPeriod+')',
                      'TR.EBITMean(Period='+iPeriod+')',
                      'TR.TaxProvisionMean(Period='+iPeriod+')',
                      'TR.NetIncomeMean(Period='+iPeriod+')',
                      'TR.RepNetProfitMean(Period='+iPeriod+')',
                      'TR.TotalDebtMean(Period='+iPeriod+')',
                      'TR.NetDebtMean(Period='+iPeriod+')',
                      'TR.Cash&EquivalentsMean(Period='+iPeriod+')',
                      'TR.CurrentAssetsMean(Period='+iPeriod+')',
                      'TR.DeferredRevenueMean(Period='+iPeriod+')',
                      'TR.CurrentLiabilitiesMean(Period='+iPeriod+')',
                      'TR.GoodwillMean(Period='+iPeriod+')',
                      'TR.InventoryMean(Period='+iPeriod+')',
                      'TR.InventorySmartEst(Period='+iPeriod+')',
                      'TR.ShareholdersEquityMean(Period='+iPeriod+')',
                      'TR.TotalAssetsMean(Period='+iPeriod+')',
                      'TR.CashFlowfromOperationsMean(Period='+iPeriod+')',
                      'TR.FCFMean(Period='+iPeriod+')',
                      'TR.IntExpMean(Period='+iPeriod+')',
                      'TR.CAPEXMean(Period='+iPeriod+')',
                      'TR.TotalDividendsMean(Period='+iPeriod+')',
                      'TR.CashFlowfromFinancingMean(Period='+iPeriod+')',
                      'TR.CashFlowfromInvestingMean(Period='+iPeriod+')',
                      'TR.NWCMean(Period='+iPeriod+')',
                      'TR.EVtoEBITDASmartEst(Period='+iPeriod+')',
                      'TR.FwdEVtoEBTSmartEst(Period='+iPeriod+')',
                      'TR.FwdEVtoEBISmartEst(Period='+iPeriod+')',
                      'TR.FwdPtoEPSSmartEst(Period='+iPeriod+')',
                      'TR.EVMean(Period='+iPeriod+')',
                      'TR.GPMMean(Period='+iPeriod+')',
                      'TR.GPMSmartEst(Period='+iPeriod+')'],
                     raw_output=True)
    return [oLabels,df]

def eikon_to_regular_ticker(iEikonTicker):
    regularTicker=iEikonTicker.split('.', 1)[0]
    #Todo check if the second split is needed
    regularTicker=regularTicker.split('.', 1)[0]
    print("regular ticker"+regularTicker)
    return regularTicker

def retrieve_estimated_fiscal_year_data(iEikonTicker):
    print("retrieve_estimated_fiscal_year_data")
    aFiscalYears=['FY1','FY2','FY3']
    aFYDataDict=collections.defaultdict(dict)
    aFYDataDict["Estimated"]="true"
    oListJson={}
    df = ek.get_data(iEikonTicker,'TR.EPSMean(Period=FY1).periodenddate',raw_output=True)

    aLastFYEnd=df['data'][0][1].split("-")
    if aLastFYEnd == ['']:
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
            aFYDataDict[aLabels[idx][1]][aLabels[idx][0]]=FloatOrZero(df['data'][0][idx+1])
        aFYJson["FY"+str(aFY1)]=dict(aFYDataDict)
        oListJson.update(copy.deepcopy(aFYJson))
        #print(oListJson)
        aFY1=aFY1+1
    return oListJson

def retrieve_fiscal_quarter_data(iEikonTicker):
    print("quarterly data")
    aFiscalQuarters=['FQ-3','FQ-2','FQ-1','FQ0','FQ1','FQ2','FQ3','FQ4']
    aFQDataDict=collections.defaultdict(dict)
    oListJson={"DataByFiscalQuarter":{}}
    for fq in aFiscalQuarters:
        aFQDataDict.clear()
        if fq in aFiscalQuarters[0:4]:
            aFQDataDict["Estimated"]="false"
            aQuarter,aQuarterEndDate = get_fiscal_quarter_end_date(iEikonTicker, fq, False)
            aLabels,df = retrieve_eikon_reports(iEikonTicker, fq)
        else:
            aFQDataDict["Estimated"]="true"
            aQuarter,aQuarterEndDate = get_fiscal_quarter_end_date(iEikonTicker, fq, True)
            aLabels,df = retrieve_eikon_estimates(iEikonTicker, fq)
        if aQuarter == '':
            #if aQuarter is empty it means there is no info for that fiscal quarter, do not add empty key
            continue
        #First elem of df is always the company name, we dont need it for len
        aDfLen=len(df['data'][0])-1
        aFYJson={str(aQuarter):{}}

        for idx in range(0,aDfLen):
            aFQDataDict[aLabels[idx][1]][aLabels[idx][0]]=FloatOrZero(df['data'][0][idx+1])
        aFYJson[str(aQuarter)]=dict(aFQDataDict)
        aFYJson[str(aQuarter)]["PeriodEndDate"]=aQuarterEndDate
        oListJson["DataByFiscalQuarter"].update(copy.deepcopy(aFYJson))
    #print(oListJson)
    return oListJson