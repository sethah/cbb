import scrapy
from bs4 import BeautifulSoup
from twisted.internet import reactor
import sys
import traceback

from DataCollection.ScrapeUtils import BoxScraper
import DataCollection.DBScrapeUtils as dbutil
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from scrapy import signals

failed = []

class BoxSpider(scrapy.Spider):
    name = "box"
    allowed_domains = ["stats.ncaa.org"]
    start_urls = dbutil.get_games_to_scrape(season=2016, from_table='box', num_games=500)

    def __init__(self):
        self.data = []
        self.failed_urls = []
        self.items = []

    def parse(self, response):
        if response.status == 404:
            self.failed_urls.append(response.url)
            print response.url
        try:
            soup = BeautifulSoup(response.body, 'html.parser')
            header_table, box_stats = BoxScraper.extract_box_stats(soup, response.url)
            if BoxScraper.is_valid_stats(box_stats):
                dbutil.insert_box_stats(box_stats)
        except:
            traceback.print_exc()
            failed.append(response.url)

def spider_closing(spider):
    """Activates on spider closed signal"""
    spider.crawler.stats.set_value('failed_urls', ','.join(spider.failed_urls))
    reactor.stop()


if __name__ == "__main__":
    spider = BoxSpider()
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
    print failed

