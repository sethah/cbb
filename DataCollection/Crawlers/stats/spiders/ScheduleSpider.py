import csv
import os
from bs4 import BeautifulSoup
from twisted.internet import reactor

from DataCollection.ScrapeUtils import ScheduleScraper
import DataCollection.DBScrapeUtils as dbutil

import scrapy
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from scrapy import signals

class ScheduleSpider(scrapy.Spider):
    name = "schedule"
    allowed_domains = ["stats.ncaa.org"]
    start_urls = ScheduleScraper.get_urls([2016])
    print start_urls

    def __init__(self):
        self.data = []
        self.failed_urls = []
        self.items = []

    def parse(self, response):
        if response.status == 404:
            self.failed_urls.append(response.url)
            print '404 error: %s' % response.url
        try:
            soup = BeautifulSoup(response.body, 'html.parser')
            games = ScheduleScraper.get_team_schedule(soup, response.url)
            item = ScheduleItem()
            item['games'] = games
            self.items.append(item)
        except Exception, e:
            print e

class ScheduleItem(scrapy.Item):
    games = scrapy.Field()

def spider_closing(spider):
    """Activates on spider closed signal"""
    # log.msg("Closing reactor", level=log.INFO)
    spider.crawler.stats.set_value('failed_urls', ','.join(spider.failed_urls))
    reactor.stop()

    with open("output.csv", "a") as f:
        writer = csv.writer(f)
        for item in spider.items:
            writer.writerows(item['games'])

if __name__ == "__main__":
    spider = ScheduleSpider()
    settings = Settings()
    settings.set('DOWNLOAD_DELAY', 0.25)
    settings.set('COOKIES_ENABLED', False)
    crawler = Crawler(spider, settings)
    crawler.crawl()
    # stop reactor when spider closes
    crawler.signals.connect(spider_closing, signal=signals.spider_closed)
    reactor.run()
    print crawler.stats.get_stats()
    dbutil.update_games_table()
    os.remove("output.csv")

