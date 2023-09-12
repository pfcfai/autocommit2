import pandas as pd
from sqlalchemy import create_engine
import pymysql

def to_db(df):  
    # engine2 = create_engine('mysql+pymysql://root:root@localhost:3306/strategy?charset=utf8')     
    engine2 = create_engine('mysql+pymysql://targets:targets@localhost:3306/strategy?charset=utf8')     
    df.to_sql('realtime', engine2, if_exists = 'append',index=False)
       
def to_db2(df):
    # engine3 = create_engine('mysql+pymysql://root:root@localhost:3306/strategy?charset=utf8')
    engine3 = create_engine('mysql+pymysql://targets:targets@localhost:3306/strategy?charset=utf8')     
    df.to_sql('linepush', engine3, if_exists = 'append',index=False)

def fr_db(id):
    engine4 = create_engine('mysql+pymysql://webmysql@actwebdb2:AIteam168@103.17.9.213:3306/strategy?charset=utf8')
    sql =f'SELECT * FROM strategy.realtime where id>{id}'
    df = pd.read_sql(sql,engine4)
    return df

def fr_db2(id):
    engine4 = create_engine('mysql+pymysql://webmysql@actwebdb2:AIteam168@103.17.9.213:3306/strategy?charset=utf8')
    sql =f'SELECT * FROM strategy.linepush where id>{id}'
    df = pd.read_sql(sql,engine4)
    return df

# 更新 realtime
# engine = create_engine('mysql+pymysql://root:root@localhost:3306/strategy?charset=utf8')
engine = create_engine('mysql+pymysql://targets:targets@localhost:3306/strategy?charset=utf8')     
sql =f'SELECT id FROM strategy.realtime order by id desc limit 1;'
df = pd.read_sql(sql,engine)
id=df.iloc[0][0]
print(f'id:{id}')

update_df=fr_db(id)
print(update_df)
to_db(update_df)

# 更新 linepush
# engine = create_engine('mysql+pymysql://root:root@localhost:3306/strategy?charset=utf8')
engine = create_engine('mysql+pymysql://targets:targets@localhost:3306/strategy?charset=utf8')     
sql =f'SELECT id FROM strategy.linepush order by id desc limit 1;'
df = pd.read_sql(sql,engine)
id=df.iloc[0][0]

update_df=fr_db2(id)
print(update_df)
to_db2(update_df)
