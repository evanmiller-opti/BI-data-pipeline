
# coding: utf-8

# In[1]:

def import_rti():
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
    import urllib

    params = urllib.quote_plus("DRIVER={SQL Server Native Client 11.0};SERVER=WLGBISQL01;DATABASE=regional_tourism;UID=evan_miller;PWD=Optimation!234")
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, echo = False)

    funcpath = 'I:/Professional/Consulting Practice/BI Practice/19. Python'
    os.chdir(funcpath)

    from helper_functions import drop_all_tables

    drop_all_tables(params)

    url = 'http://www.mbie.govt.nz/info-services/sectors-industries/tourism/tourism-research-data/regional-tourism-indicators/detailed-data'
    r  = requests.get(url)

    data = r.text

    soup = BeautifulSoup(data, "lxml")
    output = soup.find_all('a', text=re.compile('XLSX'))

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

        string = strings[start:end].lower().replace(' ', '')
        string = string.replace('pivottableof', '')

        tables.append(string)

    url_long = []
    for urltemp in urls:

        url_str = url + str(urltemp)
        url_long.append(url_str)

    count = 0

    datapath = 'I:/Professional/Consulting Practice/BI Practice/18. Data/MBIE/RTI'
    os.chdir(datapath)

    wb_name = []

    for table in tables:
        tbl = table + '.xlsx'
        wb_name.append(tbl)

    for url in url_long:

        table = tables[count]

        import urllib2
        from openpyxl import load_workbook
        import StringIO

        req = urllib2.Request(url=url_long[count])
        resp = urllib2.urlopen(req)
        xlsx =  resp.read()

        wb = load_workbook(StringIO.StringIO(xlsx))
        wb.save(wb_name[count])
        count = count + 1

    count = 0

    for wb in wb_name:
        xl = pd.ExcelFile(wb)
        xl.sheet_names
        df = xl.parse('Data base')
        df['timestamp'] = date

        tbl = tables[count]

        df.to_sql(name = tbl, con=engine, if_exists = 'replace', index=False)
        count = count + 1

        print tbl + " successfully uploaded!"

