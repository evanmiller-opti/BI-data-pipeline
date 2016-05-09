
# coding: utf-8

# In[4]:

def MBIE_SQL():

    from bs4 import BeautifulSoup
    import re
    import pandas as pd
    import requests, zipfile, StringIO
    import io
    import time
    import sqlalchemy
    import os

    date = time.strftime("%d-%m-%Y")

    from sqlalchemy import create_engine
    from sqlalchemy.engine.reflection import Inspector
    from sqlalchemy import MetaData
    import pyodbc
    import urllib2

    params = urllib2.quote("DRIVER={SQL Server Native Client 11.0};SERVER=WLGBISQL01;DATABASE=MBIE;UID=evan_miller;PWD=Optimation!234")
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

    function_path = 'I:/Professional/Consulting Practice/BI Practice/19. Python'
    os.chdir(function_path)

    from helper_functions import drop_all_tables
    drop_all_tables(params)

    url_prefix = 'http://www.mbie.govt.nz/info-services/housing-property/sector-information-and-statistics/rental-bond-data'
    r  = requests.get(url_prefix)

    data = r.text

    soup = BeautifulSoup(data, "lxml")
    output = soup.find_all('a', text=re.compile('CSV'))

    output_str = []
    for link in output:
        string = str(link)
        output_str.append(string)

    urls = []
    tables = []

    for strings in output_str:

        start = [m.start() for m in re.finditer(r'[.]', strings)][0]
        end = [m.start() for m in re.finditer(r'"', strings)][1]

        string = strings[start:end].lower().replace('.', '')

        urls.append(string)  

        start = [m.start() for m in re.finditer(r'>', strings)][0] + 1
        end = strings.index('[') - 1

        string = strings[start:end].lower().replace(" ", "")

        tables.append(string)

    count = 0

    datapath = 'I:/Professional/Consulting Practice/BI Practice/18. Data/MBIE'
    os.chdir(datapath)

    for url in urls:

        table = tables[count]

        req = urllib2.Request(url=url_prefix + url)
        resp = urllib2.urlopen(req)
        csv =  resp.read()

        csvstr = str(csv).strip("b'")

        csv_name = table + '.csv'

        lines = csvstr.split('\\n')
        f = open(csv_name, 'w')
        for line in lines:
           f.write(line + '\n')
        f.close()

        data = pd.read_csv(csv_name, low_memory=False)
        data['timestamp'] = date
        
        data.rename(columns=lambda x: x.split('.')[0], inplace=True)
        
        if 'Month' in data.columns:
            data_melt = pd.melt(data, id_vars=['Month'], var_name='market_area')
            
        else:
            data_melt = data
            
        data_melt.to_sql(name=table, con=engine, if_exists = 'replace', index=False)
        count = count + 1

        print table + " successfully uploaded!"

