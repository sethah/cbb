import csv
import os
import re
import numpy as np
import pandas as pd

import scrapy
from bs4 import BeautifulSoup
from twisted.internet import reactor

# print os.environ['PYTHONPATH'].split(os.pathsep)
from DataCollection.ScrapeUtils import DivisionOneScraper
import DataCollection.DBScrapeUtils as dbutil
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from scrapy import signals

ALL_YEARS = [2010, 2011, 2012, 2013, 2014, 2015]
# ALL_YEARS = [2010]

class DivisionOneTeamsSpider(scrapy.Spider):
    name = "box"
    allowed_domains = ["stats.ncaa.org"]
    start_urls = DivisionOneScraper.get_urls(ALL_YEARS)
    print start_urls

    def __init__(self):
        self.data = []
        self.failed_urls = []
        self.items = []
        self.teams = {year: None for year in ALL_YEARS}

    def parse(self, response):
        if response.status == 404:
            self.failed_urls.append(response.url)
            print response.url
        pattern = 'academic_year=[0-9]+'
        year = int(re.search(pattern, response.url).group().split('=')[-1])
        soup = BeautifulSoup(response.body, 'html.parser')
        ncaaids, ncaa_names = DivisionOneScraper.extract_teams(soup)
        # print ncaaids
        # self.teams[2010] = {'ids': ncaaids, 'names': ncaa_names,
        #                     'year': np.ones(ncaaids.shape[0]) * year}
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
    crawler = Crawler(spider, Settings())
    crawler.crawl()
    print "______"
    # stop reactor when spider closes
    crawler.signals.connect(spider_closing, signal=signals.spider_closed)
    reactor.run()
    print crawler.stats.get_stats()

