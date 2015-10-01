import csv
import os

import scrapy
from bs4 import BeautifulSoup
import psycopg2
from twisted.internet import reactor

print os.environ['PYTHONPATH'].split(os.pathsep)
from DataCollection.ScrapeUtils import ScheduleScraper, BoxScraper
import DataCollection.DBScrapeUtils as dbutil
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from scrapy import signals

urls = ["http://stats.ncaa.org/game/box_score/170096"]

class BoxSpider(scrapy.Spider):
    name = "box"
    allowed_domains = ["stats.ncaa.org"]
    start_urls = dbutil.get_games_to_scrape(2013, 'box', 1000)
    print start_urls

    def __init__(self):
        self.data = []
        self.failed_urls = []
        self.items = []

    def parse(self, response):
        if response.status == 404:
            self.failed_urls.append(response.url)
            print response.url
        soup = BeautifulSoup(response.body, 'html.parser')
        header_table, box_stats = BoxScraper.extract_box_stats(soup, response.url)
        dbutil.insert_box_stats(box_stats)

def spider_closing(spider):
    """Activates on spider closed signal"""
    # log.msg("Closing reactor", level=log.INFO)
    spider.crawler.stats.set_value('failed_urls', ','.join(spider.failed_urls))
    reactor.stop()


if __name__ == "__main__":
    spider = BoxSpider()
    crawler = Crawler(spider, Settings())
    crawler.crawl()
    print "______"
    # stop reactor when spider closes
    crawler.signals.connect(spider_closing, signal=signals.spider_closed)
    reactor.run()
    print crawler.stats.get_stats()

