from math import isnan
import pandas as pd
import numpy as np
import pymysql
# create_engine("mysql+mysqldb://root:tjrdnjs1!@localhost:3306/DB", encoding='utf-8')  

conn = pymysql.connect(host = 'localhost',
                       port = 3306,
                       user = 'root',
                       password = 'tjrdnjs1!',
                       db = 'DB')

sql_input = "SELECT * FROM apt_trade4"
df_this_data = pd.read_sql_query(sql_input, conn)

state_info_status = pd.read_csv('state_info_status.csv')

collected_info = df_this_data.groupby(['address_1','address_2'])['trade_date'].agg([min,max]).reset_index()

collected_info2 = pd.merge(state_info_status, collected_info, left_on=['state','district'], right_on=['address_1','address_2'], how='left')
collected_info3 = collected_info2[['code','state','district']]
collected_info3['min_date'] = collected_info2.apply(lambda x : x['min'] if (np.isnan(x['min_date'])) or (x['min'] < x['min_date']) else x['min_date'], axis=1)
collected_info3['max_date'] = collected_info2.apply(lambda x : x['max'] if (np.isnan(x['max_date'])) or (x['max'] < x['max_date']) else x['max_date'], axis=1)
collected_info3['status'] = collected_info3.apply(lambda x : 'False' if x[['min_date','max_date']].isnull().sum() != 0 else 'True', axis=1)

conn.close()
