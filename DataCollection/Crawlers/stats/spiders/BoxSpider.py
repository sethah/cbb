import scrapy
from bs4 import BeautifulSoup
from twisted.internet import reactor

from DataCollection.ScrapeUtils import BoxScraper
import DataCollection.DBScrapeUtils as dbutil
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from scrapy import signals

failed = []

class BoxSpider(scrapy.Spider):
    name = "box"
    allowed_domains = ["stats.ncaa.org"]
    start_urls = dbutil.get_games_to_scrape(season=2016, from_table='box', num_games=1000)

    def __init__(self):
        self.data = []
        self.failed_urls = []
        self.items = []

    def parse(self, response):
        if response.status == 404:
            self.failed_urls.append(response.url)
            print response.url
        soup = BeautifulSoup(response.body, 'html.parser')
        try:
            header_table, box_stats = BoxScraper.extract_box_stats(soup, response.url)
        except Exception, e:
            print e
            failed.append(response.url)
        dbutil.insert_box_stats(box_stats)

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

