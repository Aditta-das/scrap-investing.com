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
        conversion = { 'K': 1000, 'M': 1000000, 'B': 1000000000, 'T': 1000000000000 }
        for i, d in enumerate(response.xpath(f'//*[@id="curr_table"]/tbody/tr')):
            date = d.xpath(f'//*[@id="curr_table"]/tbody/tr[{i}]/td[1]/text()').extract_first()
            price = d.xpath(f'//*[@id="curr_table"]/tbody/tr[{i}]/td[2]/text()').extract_first()
            open = d.xpath(f'//*[@id="curr_table"]/tbody/tr[{i}]/td[3]/text()').extract_first()
            high = d.xpath(f'//*[@id="curr_table"]/tbody/tr[{i}]/td[4]/text()').extract_first()
            low = d.xpath(f'//*[@id="curr_table"]/tbody/tr[{i}]/td[5]/text()').extract_first()
            volume = d.xpath(f'//*[@id="curr_table"]/tbody/tr[{i}]/td[6]/text()').extract_first()
            change = d.xpath(f'//*[@id="curr_table"]/tbody/tr[{i}]/td[7]/text()').extract_first()
            vol = '0.00K'
            if volume == '-':
                volume = vol

            # if volume[-1] in conversion.keys():
            #     volume = str(float(volume[:-1]) * conversion[volume[-1]])
            # else:
            #     volume = volume

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
        df = pd.DataFrame(item, columns=['date', 'price', 'open', 'high', 'low', 'volume', 'change'])
        # df.dropna(inplace=True)
        self.log(f'Data Shape: {df.shape}, {df.shape[0]}, {df["volume"].iloc[1]}')
        for i in range(1, df.shape[0]):
            if df['volume'].iloc[i][-1] in conversion.keys():
                df['volume'].iloc[i] = float(df['volume'].iloc[i][:-1]) * conversion[df['volume'].iloc[i][-1]]
            else:
                pass
        df.to_csv (f'{newpath}/{name}.csv', index = False)
        return item