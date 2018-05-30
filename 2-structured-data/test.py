from lxml import html
import http.client, urllib.request, urllib.parse, urllib.error, base64, json, time, datetime, math, requests, calendar,copy, sys
import eikon as ek
import pandas as pd
from pprint import pprint
from datetime import datetime
from flask import current_app, Flask, redirect, url_for

ek.set_app_id('9FB32FA719C8F1EE8CDEF1A')
pd.options.display.max_colwidth = 10000

def FloatOrZero(value):
    try:
        if math.isnan(value):
            return 0.0
        else:
            return float(value)
    except:
        return 0.0

def get_minority_interest(iEikonTicker):
    start = time.time()
    iPeriod='FY-1'
    iPeriod2='FY-2'
    aMinInterest={}
    df = ek.get_data(iEikonTicker, ['TR.EBITDAActValue(Period='+iPeriod2+')',
                                    'TR.EBITActValue(Period='+iPeriod2+')',
                                    'TR.TotalRevenue(Period='+iPeriod2+')',
                                    'TR.DilutedEpsExclExtra(Period='+iPeriod2+')',
                                    'TR.DilutedEpsInclExtra(Period='+iPeriod2+')',
                                    'TR.DilutedWghtdAvgShares(Period='+iPeriod2+')',
                                    'TR.CashAndSTInvestments(Period='+iPeriod2+')',
                                    'TR.TotalDebtOutstanding(Period='+iPeriod2+')',
                                    'TR.NetDebt(Period='+iPeriod2+')',
                                    'TR.MinorityInterestBSStmt(Period='+iPeriod2+')',
                                    'TR.DpsCommonStock(Period='+iPeriod2+')',
                                    'TR.HistEnterpriseValueEBITDA(Period='+iPeriod2+')',
                                    'TR.EVEBIT(Period='+iPeriod2+')',
                                    'TR.HistEnterpriseValue(Period='+iPeriod2+')',
                                    'TR.HistPE(Period='+iPeriod2+')',
                                    'TR.GrossProfit(Period='+iPeriod2+')',
                                    'TR.NetIncome(Period='+iPeriod2+')',
                                    'TR.NetIncomeBeforeExtraItems(Period='+iPeriod2+')',
                                    'TR.ProvisionForIncomeTaxes(Period='+iPeriod2+')',
                                    'TR.DiscontinuedOperations(Period='+iPeriod2+')',
                                    'TR.GrossMargin(Period='+iPeriod2+')',
                                    'TR.EBITMarginPercent(Period='+iPeriod2+')',
                                    'TR.EBITDAMarginPercent(Period='+iPeriod2+')',
                                    'TR.CashFromOperatingActivities(Period='+iPeriod2+')',
                                    'TR.FreeCashFlow(Period='+iPeriod2+')',
                                    'TR.CapitalExpenditures(Period='+iPeriod2+')',
                                    'TR.CashInterestPaid(Period='+iPeriod2+')',
                                    'TR.CashTaxesPaid(Period='+iPeriod2+')',
                                    'TR.ChangesInWorkingCapital(Period='+iPeriod2+')',
                                    'TR.AcquisitionOfBusiness(Period='+iPeriod2+')',
                                    'TR.SaleOfBusiness(Period='+iPeriod2+')',
                                    'TR.PurchaseOfInvestments(Period='+iPeriod2+')',
                                    'TR.LTDebtReduction(Period='+iPeriod2+')',
                                    'TR.STDebtReduction(Period='+iPeriod2+')',
                                    'TR.SaleMaturityofInvestment(Period='+iPeriod2+')',
                                    'TR.RepurchaseRetirementOfCommon(Period='+iPeriod2+')',
                                    'TR.TotalCashDividendsPaid(Period='+iPeriod2+')',
                                    'TR.NetCashEndingBalance(Period='+iPeriod2+')',
                                    'TR.NetCashBeginningBalance(Period='+iPeriod2+')',
                                    'TR.CashFromOperatingActivities(Period='+iPeriod2+')',
                                    'TR.CashFromInvestingActivities(Period='+iPeriod2+')',
                                    'TR.CashFromFinancingActivities(Period='+iPeriod2+')',
                                    'TR.CashandEquivalents(Period='+iPeriod2+')',
                                    'TR.AcctsReceivTradeNet(Period='+iPeriod2+')',
                                    'TR.TotalInventory(Period='+iPeriod2+')',
                                    'TR.IntangiblesNet(Period='+iPeriod2+')',
                                    'TR.PropertyPlantEquipmentTotalNet(Period='+iPeriod2+')',
                                    'TR.GoodwillNet(Period='+iPeriod2+')',
                                    'TR.LTInvestments(Period='+iPeriod2+')',
                                    'TR.OtherCurrentAssets(Period='+iPeriod2+')',
                                    'TR.TotalAssetsReported(Period='+iPeriod2+')',
                                    'TR.AccountsPayable(Period='+iPeriod2+')',
                                    'TR.TotalLiabilities(Period='+iPeriod2+')',
                                    'TR.TotalLTDebt(Period='+iPeriod2+')',
                                    'TR.CurrentPortionLTDebtToCapitalLeases(Period='+iPeriod2+')',
                                    'TR.TotalLiabilitiesAndShareholdersEquity(Period='+iPeriod2+')',
                                    'TR.EBITDAActValue(Period='+iPeriod+')',
                                    'TR.EBITActValue(Period='+iPeriod+')',
                                    'TR.TotalRevenue(Period='+iPeriod+')',
                                    'TR.DilutedEpsExclExtra(Period='+iPeriod+')',
                                    'TR.DilutedEpsInclExtra(Period='+iPeriod+')',
                                    'TR.DilutedWghtdAvgShares(Period='+iPeriod+')',
                                    'TR.CashAndSTInvestments(Period='+iPeriod+')',
                                    'TR.TotalDebtOutstanding(Period='+iPeriod+')',
                                    'TR.NetDebt(Period='+iPeriod+')',
                                    'TR.MinorityInterestBSStmt(Period='+iPeriod+')',
                                    'TR.DpsCommonStock(Period='+iPeriod+')',
                                    'TR.HistEnterpriseValueEBITDA(Period='+iPeriod+')',
                                    'TR.EVEBIT(Period='+iPeriod+')',
                                    'TR.HistEnterpriseValue(Period='+iPeriod+')',
                                    'TR.HistPE(Period='+iPeriod+')',
                                    'TR.GrossProfit(Period='+iPeriod+')',
                                    'TR.NetIncome(Period='+iPeriod+')',
                                    'TR.NetIncomeBeforeExtraItems(Period='+iPeriod+')',
                                    'TR.ProvisionForIncomeTaxes(Period='+iPeriod+')',
                                    'TR.DiscontinuedOperations(Period='+iPeriod+')',
                                    'TR.GrossMargin(Period='+iPeriod+')',
                                    'TR.EBITMarginPercent(Period='+iPeriod+')',
                                    'TR.EBITDAMarginPercent(Period='+iPeriod+')',
                                    'TR.CashFromOperatingActivities(Period='+iPeriod+')',
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
                                    'TR.CashFromFinancingActivities(Period='+iPeriod+')',
                                    'TR.CashandEquivalents(Period='+iPeriod+')',
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
                                    'TR.TotalLTDebt(Period='+iPeriod+')',
                                    'TR.CurrentPortionLTDebtToCapitalLeases(Period='+iPeriod+')',
                                    'TR.TotalLiabilitiesAndShareholdersEquity(Period='+iPeriod+')'
                                    ], raw_output=True)
    end = time.time()
    print(end - start)
    return aMinInterest


if __name__ == '__main__':
    a = get_minority_interest("AAPL.O")
    file = open('C:/Users/ant1_/Documents/Git Repos/getting-started-python/2-structured-data/testfile.txt','w')
    file.write(json.dumps(a))
    file.close()
    print('result')
    print(a)


