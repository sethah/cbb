import scrapy
from bs4 import BeautifulSoup
from twisted.internet import reactor

from DataCollection.ScrapeUtils import KenpomScraper
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from scrapy import signals

YEARS = range(2002, 2016)

class KenpomSpider(scrapy.Spider):
    name = "box"
    allowed_domains = ["kenpom.com"]
    start_urls = KenpomScraper.get_urls(YEARS)

    def __init__(self):
        self.data = []
        self.failed_urls = []
        self.items = []

    def parse(self, response):
        if response.status == 404:
            self.failed_urls.append(response.url)
            print response.url
        soup = BeautifulSoup(response.body, 'html.parser')
        year = KenpomScraper.get_year(response.url)
        df = KenpomScraper.extract_teams(soup, year)
        KenpomScraper.insert_data(df)

def spider_closing(spider):
    """Activates on spider closed signal"""
    spider.crawler.stats.set_value('failed_urls', ','.join(spider.failed_urls))
    reactor.stop()


if __name__ == "__main__":
    spider = KenpomSpider()
    crawler = Crawler(spider, Settings())
    crawler.crawl()
    print "______"
    # stop reactor when spider closes
    crawler.signals.connect(spider_closing, signal=signals.spider_closed)
    reactor.run()
    print crawler.stats.get_stats()

