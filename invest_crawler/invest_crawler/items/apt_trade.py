import scrapy

class AptTradeScrapy(scrapy.Item):    
    apt_name = scrapy.Field()    
    address_1 = scrapy.Field()    
    address_2 = scrapy.Field()    
    address_3 = scrapy.Field()    
    address_4 = scrapy.Field()    
    address = scrapy.Field()    
    age = scrapy.Field()    
    floor = scrapy.Field()    
    available_space = scrapy.Field()    
    trade_date = scrapy.Field()    
    trade_amount = scrapy.Field()

    def to_dict(self):        
        return {            
            'apt_name': self['apt_name'],            
            'address_1': self['address_1'],            
            'address_2': self['address_2'],            
            'address_3': self['address_3'],            
            'address_4': self['address_4'],            
            'address': self['address'],            
            'age': self['age'],            
            'floor': self['floor'],            
            'available_space': self['available_space'],            
            'trade_date': self['trade_date'],            
            'trade_amount': self['trade_amount']        
            }
