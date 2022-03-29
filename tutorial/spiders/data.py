from dataclasses import replace
from gc import callbacks
from fsspec import Callback
import scrapy
from scrapy import Request
import pandas as pd
from scrapy.http import FormRequest
import os

class ListScraper(scrapy.Spider):
    name = "data"
    start_urls = ["https://www.investing.com/equities/bangladesh"]

    def parse(self, response): 
        for f_s in response.css('.bold.left.noWrap.elp.plusIconTd a::attr(href)').extract():
            new_link = 'https://www.investing.com/' + f_s + '-historical-data'
            yield Request (
                response.urljoin(new_link),
                callback=self.parse_stock_data,
            )

    def parse_stock_data(self, response):
        name = response.xpath('//*[@id="quotes_summary_current_data"]/div[2]/div[4]/span[2]/text()').get()
        self.log(name)
        item = []
        for i, d in enumerate(response.xpath(f'//*[@id="curr_table"]/tbody/tr')):
            date = d.xpath(f'//*[@id="curr_table"]/tbody/tr[{i}]/td[1]/text()').extract_first()
            price = d.xpath(f'//*[@id="curr_table"]/tbody/tr[{i}]/td[2]/text()').extract_first()
            open = d.xpath(f'//*[@id="curr_table"]/tbody/tr[{i}]/td[3]/text()').extract_first()
            high = d.xpath(f'//*[@id="curr_table"]/tbody/tr[{i}]/td[4]/text()').extract_first()
            low = d.xpath(f'//*[@id="curr_table"]/tbody/tr[{i}]/td[5]/text()').extract_first()
            volume = d.xpath(f'//*[@id="curr_table"]/tbody/tr[{i}]/td[6]/text()').extract_first()
            change = d.xpath(f'//*[@id="curr_table"]/tbody/tr[{i}]/td[7]/text()').extract_first()
            
            item.append({
                'date': date,
                'price': price,
                'open': open,
                'high': high,
                'low': low,
                'volume': volume,
                'change': change
            })
        
        newpath = name
        if not os.path.exists(newpath):
            os.makedirs(newpath)
        df = pd.DataFrame.from_dict(item)
        df.to_csv (f'{newpath}/{name}.csv', index = False, header=True)
        return item