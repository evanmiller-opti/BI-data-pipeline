def stats_import():

    from bs4 import BeautifulSoup
    import re
    import pandas as pd
    import requests, zipfile, StringIO
    import io
    import time
    import sqlalchemy
    import os

    date = time.strftime("%d-%m-%Y")
    building_consents_biz_demog = True

    from sqlalchemy import create_engine
    from sqlalchemy.engine.reflection import Inspector
    from sqlalchemy import MetaData
    import pyodbc
    import urllib

    params = urllib.quote_plus("DRIVER={SQL Server Native Client 11.0};SERVER=WLGBISQL01;DATABASE=stats_nz;UID=evan_miller;PWD=Optimation!234")
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

    ttl_names = []

    def filterOut(input_data, reg1, reg2):

        full = input_data
        regex = re.compile(reg1)
        filtered = filter(lambda i: not regex.search(i), full)
        
        regex = re.compile(reg2)
        filtered = filter(lambda i: not regex.search(i), filtered)
        return filtered

    def tbl_name(url):
        
        inspector = Inspector.from_engine(engine)
        tbl_names = inspector.get_table_names()
        
        extract = url.split('/')
            
        if extract[2] in ['www3.stats.govt.nz']:
            name = extract[4]
        else:
            name = extract[7]
        
        ttl_names.append(name)
        
        if ttl_names.count(name) > 1:
            
            final_name = name + '_' + str(ttl_names.count(name))
            
        else:
            
            final_name = name
            
        return(final_name)

    def open_zip_save(zip, url):
        file_names = pd.Series(zip.namelist())
        for name in file_names:
                    
            out_name = tbl_name(url)
            data = pd.read_csv(zip.open(name))
            data['timestamp'] = pd.Series(date, index=data.index)
            data['file_name'] = pd.Series(name.replace("-csv", ""), index=data.index)
            
            new_string = ''
            
            if re.search("_", out_name) is not None:
                pos = out_name.index('_') + 1
                new_string = out_name[0:pos] + name.replace("-csv", "")
                new_string = new_string.replace(".csv", "")
            
            else:
                new_string = out_name
            
            data.to_sql(name=new_string, con=engine, if_exists = 'replace', index=False)
            
            print "Table: " + new_string + " has been successfully uploaded"
                
    def save_all_zips_to_sql(urls):
        for url in urls:
            r = requests.get(url)
            z = zipfile.ZipFile(StringIO.StringIO(r.content))

            file_names = pd.Series(z.namelist())
            open_zip_save(z, url) 
            
    url = "http://www.stats.govt.nz/tools_and_services/releases_csv_files.aspx"
    url_short = "http://www.stats.govt.nz"
    r  = requests.get(url)

    data = r.text

    soup = BeautifulSoup(data, "lxml")
    output = []

    for link in soup.find_all('a', href = re.compile('.zip')):
        link = link['href']
        output.append(link)

    output_df = pd.Series(output)
    output_df_int = output_df.str.replace(" ", "%")
    output_df_final = output_df_int.str.replace("%for%", "%20for%20")
    output_df_final_1 = output_df_final.str.replace("%csv%tables", "%20csv%20tables")

    file_url = []

    for url in output_df_final_1:
        if url[0:4] in ["http"]:
           fileurl = url
           file_url.append(fileurl)
        else:
            fileurl = url_short + url
            file_url.append(fileurl)
            
    if building_consents_biz_demog:
        clean_file_url = filterOut(file_url, r'BusDem', r'BuildingConsentsIssued')
    else: 
        clean_file_url = file_url
        
    save_all_zips_to_sql(clean_file_url)