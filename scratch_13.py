import xlwings as xw
import pandas as pd
from simple_salesforce import Salesforce
import urllib3
import json, requests
from pandas.io.json import json_normalize
import simple_salesforce
import os
import browser_cookie3
from datetime import datetime
from datetime import timedelta
import timeit
import numpy
from dateutil.relativedelta import *
import requests
import csv
from io import StringIO
import regex as re
from collections import OrderedDict
import warnings
warnings.filterwarnings('ignore')
import threading
from queue import Queue
import webbrowser

###########################################################################
# Step 0: Get Cookie / SID for API connections

# GET COOKIE AND CONNECT TO SALESFORCE
domain = 'org62.my.salesforce.com'
cj = browser_cookie3.chrome(domain_name=domain)
my_cookies = requests.utils.dict_from_cookiejar(cj)
if len(my_cookies) == 0:
    sys.exit('ERROR: Could not get session ID.  Make sure you are logged into a live Salesforce session.')
else:
    pass

# start salesforce connection using simple salesforce
sf = Salesforce(instance=domain, session_id=my_cookies['sid'])

# setup request headers and env_url for use with Einstein Analytics API
env_url = 'https://org62.my.salesforce.com'
h = {'Authorization': 'Bearer ' + my_cookies['sid'], 'Content-Type': 'application/json'}

###########################################################################

def SOQL_breakup(id, query):

    order_list = []
    for x in id:
        x = str(x)
        order_list.append(x)
    order_list = [order_list[i: i + 200] for i in range(0, len(order_list), 200)]

    final_df = pd.DataFrame()

    try:

        for x in order_list:
            if len(final_df) == 0:
                soql_report = query + """(""" + str(x)[1:-1] + """)"""
                final_df = pd.DataFrame(sf.query_all(soql_report)['records']).drop(columns=['attributes'])
                final_df = soql_converter_recursion(final_df, [])
            else:
                soql_report = query + """(""" + str(x)[1:-1] + """)"""
                try:
                    temp = pd.DataFrame(sf.query_all(soql_report)['records']).drop(columns=['attributes'])
                    temp = soql_converter_recursion(temp, [])
                    final_df = pd.concat([final_df, temp], ignore_index=True, sort=True)
                except:
                    print('No data returned')
    except:
        print('No data')

    return final_df

def soql_converter_recursion(df, used_col_headers=[]):

    ordered_dict_col_headers = []
    list_col_headers = []

    for column in df.columns:
        if column in used_col_headers:
            next
        else:
            if numpy.any([isinstance(val, dict) for val in df[column]]):
                ordered_dict_col_headers.append(column)
                used_col_headers.append(column)
            elif numpy.any([isinstance(val, list) for val in df[column]]):
                continue
            else:
                used_col_headers.append(column)

    for column in ordered_dict_col_headers:
        null_row = True
        k = 0
        while null_row:
            if df[column][k] is not None:
                null_row = False
            else:
                k += 1
        keys = list(df[column][k])
        for key in keys:
            try:
                df[column + "_" + key] = df[column].apply(lambda x: x.get(key) if x is not None else None)
            except Exception as e:
                print("Error: ", e)
                next

    for column in df.columns:
        if column in used_col_headers:
            next
        else:
            if numpy.any([isinstance(val, list) for val in df[column]]):
                list_col_headers.append(column)
                used_col_headers.append(column)

    for column in list_col_headers:
        for i in range(0, (len(df[column]))):
            if df[column][i] is not None:
                for j in range(0, (len(df[column][i]))):
                    if issubclass(type(df[column][i][j]), dict):
                        keys = list(df[column][i][j])
                        for key in keys:
                            try:
                                df[column + "_" + str(j) + "_" + key] = df[column].apply(lambda x: x[j].get(key) if x is not None and j in range(0, len(x)) else None)
                            except Exception as e:
                                print("Error on i = :" + str(i) + " j = " + str(j))
                                print(e)
                                print(column)
                                print(key)
                                next

    if len(ordered_dict_col_headers) == 0 and len(list_col_headers) == 0:
        return df
    else:
        return soql_converter_recursion(df, used_col_headers=used_col_headers)

def offline_comment(data):

    pattern = ['offline']

    # print(data['Order_Commission_Audit_Comments__c'])
    offline_comment_search = re.findall(r'offline', data['Order__r.Commission_Audit_Comments__c'], re.IGNORECASE)
    if len(offline_comment_search) > 0:
        return 'Include'
    else:
        return 'Exclude'

offline_renewals = pd.read_csv(r'C:\Users\nwiand\Desktop\offline_renewal.csv')

offline_renewals = offline_renewals[(offline_renewals['Commissionable_ACV__c'] >= 0)]

offline_renewals = offline_renewals[(offline_renewals['CommissionableAOV__c'] >= 0)]

offline_renewals['Commissionable_ACV__c_AGG'] = offline_renewals.groupby(['Order__r.Id'])['Commissionable_ACV__c'].transform('sum')

offline_renewals['CommissionableAOV__c_AGG'] = offline_renewals.groupby(['Order__r.Id'])['CommissionableAOV__c'].transform('sum')

offline_renewals = offline_renewals.drop_duplicates('Order__r.Id')

offline_renewals = offline_renewals[['Order__r.Id', 'Order__r.EffectiveDate',
       'Order__r.EndDate', 'Order__r.Order_Sub_Type__c',
       'Order__r.GEO_Activated_Date__c',
       'Order__r.Commission_Audit_Comments__c', 'Commissionable_ACV__c',
       'CommissionableAOV__c', 'Commissionable_ACV__c_AGG',
       'CommissionableAOV__c_AGG']]

offline_renewals['Order__r.Commission_Audit_Comments__c'] = offline_renewals['Order__r.Commission_Audit_Comments__c'].fillna('Blank')

offline_renewals['Include/Exclude'] = offline_renewals.apply(lambda x: offline_comment(x), axis=1)

offline_renewals = offline_renewals[offline_renewals['Include/Exclude'] == 'Include']

order_query = """

SELECT Id, CurrencyIsoCode
from Order

Where Id in

"""

DATA = {'Unit Price Currency': ['AUD', 'BRL', 'CAD', 'EUR', 'GBP', 'JPY', 'SEK', 'USD'],
        'Rate': [1.369863, 5.365008, 1.305735, 0.840336, 0.757576, 102.997219, 8.640055, 1]}
CURRENCY_DF = pd.DataFrame(DATA)

order_data = SOQL_breakup((offline_renewals['Order__r.Id'].values.tolist()), order_query)

offline_renewals = pd.merge(offline_renewals, order_data, how='left', left_on='Order__r.Id', right_on='Id')

offline_renewals = pd.merge(offline_renewals, CURRENCY_DF, how='left', left_on='CurrencyIsoCode', right_on='Unit Price Currency')

offline_renewals['Commissionable_ACV__c_AGG'] = offline_renewals['Commissionable_ACV__c_AGG'] / offline_renewals['Rate']

offline_renewals['CommissionableAOV__c_AGG'] = offline_renewals['CommissionableAOV__c_AGG'] / offline_renewals['Rate']

offline_renewals.to_csv(r'C:\Users\nwiand\Desktop\Offline_renewals_test2.csv')




