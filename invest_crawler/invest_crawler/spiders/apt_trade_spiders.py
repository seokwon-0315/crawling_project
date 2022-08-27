import datetime as dt
from urllib.parse import urlencode
import scrapy 
from scrapy import Selector
from dateutil.relativedelta import relativedelta
import invest_crawler.consts as CONST
from invest_crawler.items.apt_trade import AptTradeScrapy
from openpyxl import Workbook
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
import warnings 
warnings.filterwarnings('ignore')


class TradeSpider(scrapy.spiders.XMLFeedSpider):
    name = 'apt_trade'

    def start_requests(self):    

        self.engine = create_engine("mysql+mysqldb://root:tjrdnjs1!@localhost:3306/DB", encoding='utf-8')     
        self.init_date = dt.datetime(2006, 1, 1)   

        for _, st_info in CONST.state_info.iterrows():
            date = self.init_date
            self.code = st_info['code']
            self.state = st_info['state']        
            self.district = st_info['district'] 

            while date <= dt.datetime.today():#dt.datetime(2022, 8, 26):
                yield from self.get_realestate_trade_data(date)
                date += relativedelta(months=1)

    def get_realestate_trade_data(self, date):
        page_num = 1 
        urls = [CONST.APT_DETAIL_ENDPOINT]
        params = {"pageNo": str(page_num),
                  "numOfRows": "999",
                  "LAWD_CD": self.code,
                  "DEAL_YMD": date.strftime("%Y%m")
                  }
        for url in urls:
            url += urlencode(params)
            yield scrapy.Request(url=url, callback=self.parse, cb_kwargs=dict(page_num=page_num, date=date))


    def parse(self, response, page_num, date):
        selector = Selector(response, type='xml')
        items = selector.xpath('//%s' % self.itertag)  # self.intertag는 기본적으로 item으로 되어 있음

        if not items:
            return

        apt_trades = [self.parse_item(item) for item in items]
        apt_dataframe = pd.DataFrame.from_records([apt_trade.to_dict() for apt_trade in apt_trades])    
        apt_dataframe['insert_ymd'] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # print(apt_dataframe)
        # 가져온 데이터를 DB에 저장
        apt_dataframe.to_sql('apt_trade5', con=self.engine, if_exists='append', index=False)

        # status info 업데이트
        self.update_status_info(apt_dataframe)


    def parse_item(self, item):        

        try:
            address_1=CONST.state_info.query(f"code == {int(item.xpath('./지역코드/text()').get())}").iloc[0]['state']
            address_2=CONST.state_info.query(f"code == {int(item.xpath('./지역코드/text()').get())}").iloc[0]['district']

            apt_trade_data = AptTradeScrapy(                
                apt_name=item.xpath("./아파트/text()").get(),                
                address_1=address_1,              
                address_2=address_2,                
                address_3=item.xpath("./법정동/text()").get().strip(),                
                address_4=item.xpath("./지번/text()").get(),                
                address= address_1 + " " + address_2 + " " + item.xpath("./법정동/text()").get().strip() + " " +                        
                        item.xpath("./지번/text()").get(),                
                age=item.xpath("./건축년도/text()").get(),                
                floor=item.xpath("./층/text()").get(),                
                available_space=item.xpath("./전용면적/text()").get(),                
                # trade_date=item.xpath("./년/text()").get() +                           
                # item.xpath("./월/text()").get() +                           
                # item.xpath("./일/text()").get(),
                trade_date = dt.datetime(int(item.xpath("./년/text()").get()), int(item.xpath("./월/text()").get()), int(item.xpath("./일/text()").get())).strftime("%Y%m%d"),                
                trade_amount=item.xpath("./거래금액/text()").get().strip().replace(',', '')
                )     

        except Exception as e:            
            print(e)            
            self.logger.error(item)            
            self.logger.error(item.xpath("./아파트/text()").get())        
            
        return apt_trade_data


    def update_status_info(self, collected_df):
        status_file_dir = 'state_info_status.csv'
        state_info_status = pd.read_csv(status_file_dir)

        collected_info = collected_df.groupby(['address_1','address_2'])['trade_date'].agg([min,max]).reset_index()
        collected_info2 = pd.merge(state_info_status, collected_info, left_on=['state','district'], right_on=['address_1','address_2'], how='left')
        collected_info3 = collected_info2[['code','state','district']]
        collected_info3['min_date'] = collected_info2.apply(lambda x : x['min'] if (np.isnan(x['min_date'])) or (float(x['min']) < float(x['min_date'])) else x['min_date'], axis=1)
        collected_info3['max_date'] = collected_info2.apply(lambda x : x['max'] if (np.isnan(x['max_date'])) or (float(x['max']) > float(x['max_date'])) else x['max_date'], axis=1)
        collected_info3['status'] = collected_info3.apply(lambda x : 'False' if x[['min_date','max_date']].isnull().sum() != 0 else 'True', axis=1)

        collected_info3.to_csv(status_file_dir, index=False, encoding='utf-8-sig')

        return True
