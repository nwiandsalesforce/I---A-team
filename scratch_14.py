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

def AA_comment(data):

    pattern = ['offline']

    # print(data['Order_Commission_Audit_Comments__c'])
    offline_comment_search = re.findall(r'AA', data['Commission_Audit_Comments__c'], re.IGNORECASE)
    if len(offline_comment_search) > 0:
        return 'Include'
    else:
        return 'Exclude'

def NRR_impact(data):

    data['NRR_Impact__c'] = data['NRR_Impact__c'].replace(",", "")


    if int(re.findall('[0-9]+', data['NRR_Impact__c'], re.IGNORECASE)[0]) < 10:
        nrr_impact_search = re.findall('[0-9]+.?', data['NRR_Impact__c'], re.IGNORECASE)
        return nrr_impact_search[0]



    nrr_impact_search = re.findall('[0-9]+.?\d+', data['NRR_Impact__c'], re.IGNORECASE)

    return nrr_impact_search[0]

add_auto = pd.read_csv(r'C:\Users\nwiand\Desktop\FY22-Q1-Q2.csv')


add_auto['Commission_Audit_Comments__c'] = add_auto['Commission_Audit_Comments__c'].fillna('Blank')

add_auto['Include/Exclude'] = add_auto.apply(lambda x: AA_comment(x), axis=1)

add_auto = add_auto[add_auto['Include/Exclude'] == 'Include']

add_auto['NRR_value'] = add_auto.apply(lambda x: NRR_impact(x), axis=1)


add_auto.to_csv(r'C:\Users\nwiand\Desktop\nrr_FY22 H1.csv')