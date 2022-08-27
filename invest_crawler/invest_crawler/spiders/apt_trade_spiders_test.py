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
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb


class TradeSpider(scrapy.spiders.XMLFeedSpider):
    name = 'trade'

    def start_requests(self):    

        self.engine = create_engine("mysql+mysqldb://root:tjrdnjs1!@localhost:3306/DB", encoding='utf-8')   
        self.code = CONST.state_info.iloc[0]['code']
        self.state = CONST.state_info.iloc[0]['state']        
        self.district = CONST.state_info.iloc[0]['district']     
        self.init_date = dt.datetime(2020, 1, 1)        
            
        yield from self.get_realestate_trade_data(self.init_date)

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
        # print(apt_dataframe)
        # 가져온 데이터를 DB에 저장
        # apt_dataframe.to_sql('APT_TRADE', con=self.engine, if_exists='append')
        apt_dataframe.to_sql('apt_trade3', con=self.engine, if_exists='append', index=False)

        # 시작날로부터 1달씩 추가하여 데이터를 가져옴
        for _, st_info in CONST.state_info.iterrows():
            if self.code != st_info['code']:
                date = self.init_date
            self.code = st_info['code']
            self.state = st_info['state']        
            self.district = st_info['district']     
            while date <= dt.datetime(2022, 8, 26):
                date += relativedelta(months=1)
                yield from self.get_realestate_trade_data(date)


    def parse_item(self, item):        

        try:            
            apt_trade_data = AptTradeScrapy(                
                apt_name=item.xpath("./아파트/text()").get(),                
                address_1=self.state,                
                address_2=self.district,                
                address_3=item.xpath("./법정동/text()").get().strip(),                
                address_4=item.xpath("./지번/text()").get(),                
                address=self.state + " " + self.district + " " + item.xpath("./법정동/text()").get().strip() + " " +                        
                        item.xpath("./지번/text()").get(),                
                age=item.xpath("./건축년도/text()").get(),                
                level=item.xpath("./층/text()").get(),                
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


