import csv
import os
import re
import numpy as np
import pandas as pd

import scrapy
from bs4 import BeautifulSoup
from twisted.internet import reactor

from DataCollection.ScrapeUtils import DivisionOneScraper
import org_ncaa
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from scrapy import signals

class DivisionOneTeamsSpider(scrapy.Spider):
    allowed_domains = ["stats.ncaa.org"]
    start_urls = DivisionOneScraper.get_urls(org_ncaa.all_years())

    def __init__(self):
        self.data = []
        self.failed_urls = []
        self.items = []

    def parse(self, response):
        if response.status == 404:
            self.failed_urls.append(response.url)
            print response.url
        pattern = 'academic_year=[0-9]+'
        year = int(re.search(pattern, response.url).group().split('=')[-1])
        soup = BeautifulSoup(response.body, 'html.parser')
        ncaaids, ncaa_names = DivisionOneScraper.extract_teams(soup)
        self.data.append((year, ncaaids, ncaa_names))

def spider_closing(spider):
    """Activates on spider closed signal"""
    dfs = []
    for year, ids, names in spider.data:
        df = pd.DataFrame(np.array([ids, names]).T, columns=["teamid", "team"])
        df['year'] = year
        dfs.append(df)
    df = pd.concat(dfs, axis=0)
    df.to_csv("/Users/sethhendrickson/cbb/tempd1.csv", index=False)
    spider.crawler.stats.set_value('failed_urls', ','.join(spider.failed_urls))
    reactor.stop()


if __name__ == "__main__":
    spider = DivisionOneTeamsSpider()
    settings = Settings()
    settings.set('DOWNLOAD_DELAY', 0.5)
    settings.set('COOKIES_ENABLED', False)
    crawler = Crawler(spider, settings)
    crawler.crawl()
    print "______"
    # stop reactor when spider closes
    crawler.signals.connect(spider_closing, signal=signals.spider_closed)
    reactor.run()
    print crawler.stats.get_stats()
    DivisionOneScraper.insert_data()

