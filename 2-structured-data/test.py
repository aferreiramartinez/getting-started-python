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
    print("min interest")
    aMinInterest={}
    df = ek.get_data(iEikonTicker, 'TR.MinorityInterestNonRedeemable', raw_output=True)
    aMinInterest["MinorityInterest"]=FloatOrZero(df['data'][0][1])
    return aMinInterest


if __name__ == '__main__':
    a = get_minority_interest("AAPL.O")
    file = open('C:/Users/ant1_/Documents/Git Repos/getting-started-python/2-structured-data/testfile.txt','w')
    file.write(json.dumps(a))
    file.close()
    print('result')
    print(a)


