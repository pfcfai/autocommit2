# -*- coding: utf-8 -*-
import pymysql
import time , datetime
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

# dbconnect
'''dbconn=pymysql.connect(
            host="103.17.9.213",
            database= "pricedata_day",
            user="webmysql@actwebdb2",
            password="AIteam168",
            port=3306,
            charset='utf8',
            )  '''
dbconn=pymysql.connect(
            host="localhost",
            database= "strategy",
            user="targets",
            password="targets",
            port=3306,
            charset='utf8',
            ) 

codelist=['FITX','DX','AD','BP','CD','EC','JY','SF','MHI','SSI','CN','IN','DXM','MYM','MES','MNQ','FITE','TY','CL','HO','NG','GC','SI','HG','PL','W','C','S','PA','CT']
try:

    for code in codelist:

        sql=f" select timenow,recent_close from strategy.linepush where  symbol_code='{code}'"
        df1 = pd.read_sql(sql=sql, con=dbconn) 

        sql2=f" select timenow,recent_close from strategy.realtime where  symbol_code='{code}'"
        df2 = pd.read_sql(sql=sql2, con=dbconn)
        #print(df2)

        # Perform an outer join on 'timenow' column
        merged_df = pd.merge(df2, df1, on='timenow', how='outer')
        # Rename the 'recent_close' columns from both dataframes
        merged_df = merged_df.rename(columns={'recent_close_x': 'realtime', 'recent_close_y': 'linepush'})
        #print(merged_df)
        merged_df.to_csv('merged.csv')

        finaldict = {}
        finaldict["Date"] = merged_df['timenow'].tolist()
        finaldict["Realtime"] = merged_df['realtime'].tolist()
        finaldict["Linepush"] = merged_df['linepush'].tolist()

        #print(finaldict)
        # Convert Timestamp objects to strings
        for key in finaldict:
            if isinstance(finaldict[key][0], pd.Timestamp):
                finaldict[key] = [str(ts) for ts in finaldict[key]]
        import json
        with open(f'/home/targets/autocommit2/EXCEL_AutoUpdate/SwissKnife/{code}.json', 'w') as json_file:
            json.dump(finaldict, json_file)
        print(f'export {code} success')
except Exception as e:
    print(e)
