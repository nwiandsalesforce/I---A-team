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

def nrr_search(data):

    try:
        nrr_search = re.findall(r'NRR', data['Subject'], re.IGNORECASE)
        if len(nrr_search) > 0:
            return 'Include'

        nrr_search = re.findall(r'NRR', data['Description'], re.IGNORECASE)

        if len(nrr_search) > 0:
            return 'Include'

        nrr_search = re.findall(r'net revenue rule', data['Subject'], re.IGNORECASE)
        if len(nrr_search) > 0:
            return 'Include'

        nrr_search = re.findall(r'net revenue rule', data['Description'], re.IGNORECASE)

        if len(nrr_search) > 0:
            return 'Include'

        nrr_search = re.findall(r'NNR', data['Subject'], re.IGNORECASE)
        if len(nrr_search) > 0:
            return 'Include'

        nrr_search = re.findall(r'NNR', data['Description'], re.IGNORECASE)

        if len(nrr_search) > 0:
            return 'Include'

        nrr_search = re.findall(r'net revenue', data['Subject'], re.IGNORECASE)
        if len(nrr_search) > 0:
            return 'Include'

        nrr_search = re.findall(r'net revenue', data['Description'], re.IGNORECASE)

        if len(nrr_search) > 0:
            return 'Include'

        data = data.fillna('Blank')

        for x in data[['Feeds.records.0.Body', 'Feeds.records.1.Body',
       'Feeds.records.2.Body', 'Feeds.records.3.Body', 'Feeds.records.4.Body',
       'Feeds.records.5.Body', 'Feeds.records.6.Body', 'Feeds.records.7.Body',
       'Feeds.records.8.Body', 'Feeds.records.9.Body', 'Feeds.records.10.Body',
       'Feeds.records.11.Body', 'Feeds.records.12.Body',
       'Feeds.records.13.Body', 'Feeds.records.14.Body',
       'Feeds.records.15.Body', 'Feeds.records.16.Body',
       'Feeds.records.17.Body', 'Feeds.records.18.Body',
       'Feeds.records.19.Body', 'Feeds.records.20.Body',
       'Feeds.records.21.Body', 'Feeds.records.22.Body',
       'Feeds.records.23.Body', 'Feeds.records.24.Body',
       'Feeds.records.25.Body', 'Feeds.records.26.Body',
       'Feeds.records.27.Body', 'Feeds.records.28.Body',
       'Feeds.records.29.Body', 'Feeds.records.30.Body',
       'Feeds.records.31.Body', 'Feeds.records.32.Body',
       'Feeds.records.33.Body', 'Feeds.records.34.Body',
       'Feeds.records.35.Body', 'Feeds.records.36.Body',
       'Feeds.records.37.Body']]:

            nrr_search = re.findall(r'NRR', x, re.IGNORECASE)
            if len(nrr_search) > 0:
                return 'Include'

            nrr_search = re.findall(r'NRR', x, re.IGNORECASE)

            if len(nrr_search) > 0:
                return 'Include'

            nrr_search = re.findall(r'net revenue rule', x, re.IGNORECASE)
            if len(nrr_search) > 0:
                return 'Include'

            nrr_search = re.findall(r'net revenue rule', x, re.IGNORECASE)

            if len(nrr_search) > 0:
                return 'Include'

            nrr_search = re.findall(r'NNR', x, re.IGNORECASE)
            if len(nrr_search) > 0:
                return 'Include'

            nrr_search = re.findall(r'NNR', x, re.IGNORECASE)

            if len(nrr_search) > 0:
                return 'Include'

            nrr_search = re.findall(r'net revenue', x, re.IGNORECASE)
            if len(nrr_search) > 0:
                return 'Include'

            nrr_search = re.findall(r'net revenue', x, re.IGNORECASE)

            if len(nrr_search) > 0:
                return 'Include'

        else:
            return 'Exclude'
    except:
        print('test')


    # print(data['Order_Commission_Audit_Comments__c'])


def nrr_(data):

    data['NRR_Impact__c'] = data['NRR_Impact__c'].replace(",", "")


    if int(re.findall('[0-9]+', data['NRR_Impact__c'], re.IGNORECASE)[0]) < 10:
        nrr_impact_search = re.findall('[0-9]+.?', data['NRR_Impact__c'], re.IGNORECASE)
        return nrr_impact_search[0]



    nrr_impact_search = re.findall('[0-9]+.?\d+', data['NRR_Impact__c'], re.IGNORECASE)

    return nrr_impact_search[0]

nrr_cases = pd.read_csv(r'C:\Users\nwiand\Desktop\FY21 Cases.csv', encoding='latin-1')

nrr_cases['Subject'] = nrr_cases['Subject'].fillna('Blank')

nrr_cases['Description'] = nrr_cases['Description'].fillna('Blank')

nrr_cases = nrr_cases[nrr_cases['Owner.Name'] != nrr_cases['CreatedBy.Name']]


nrr_cases['Include/Exclude'] = nrr_cases.apply(lambda x: nrr_search(x), axis=1)

nrr_cases = nrr_cases[nrr_cases['Include/Exclude'] == 'Include']

nrr_cases = nrr_cases[nrr_cases['Subject'] != 'Application of NRR - Semi-automation process']

# nrr_cases['NRR_value'] = nrr_cases.apply(lambda x: NRR_impact(x), axis=1)

crediting_nrr_cases = nrr_cases[nrr_cases['Owner.Name'].isin(['Cory Gault', 'David Page', 'Nick Riddle', 'Elise Likens', 'Tyler Porter', 'Paul Johnson', 'Curtis Boyd', 'Maryann Kirkhoff', 'Jasmine Lawson', 'Jas Sandhu', 'Brooke Cass', 'Thomas Tong', 'Colleen Dierkes', 'Jordan Bontrager', 'Stephen Fountain'])]

bp_nrr_cases = nrr_cases[~nrr_cases['Owner.Name'].isin(['Cory Gault', 'David Page', 'Nick Riddle', 'Elise Likens', 'Tyler Porter', 'Paul Johnson', 'Curtis Boyd', 'Maryann Kirkhoff', 'Jasmine Lawson', 'Jas Sandhu', 'Brooke Cass', 'Thomas Tong', 'Colleen Dierkes', 'Jordan Bontrager', 'Stephen Fountain'])]

print(len(crediting_nrr_cases))
print(len(bp_nrr_cases))

crediting_nrr_cases.to_csv(r'C:\Users\nwiand\Desktop\fy21nrrcrediting.csv')
bp_nrr_cases.to_csv(r'C:\Users\nwiand\Desktop\fy21nrrBP.csv')