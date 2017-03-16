import scrapy
from bs4 import BeautifulSoup
from twisted.internet import reactor
import sys, getopt
import traceback

from DataCollection.ScrapeUtils import BoxScraper
import DataCollection.DBScrapeUtils as dbutil
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher

def parse_args(args):
    try:
        opts, args = getopt.getopt(args,"hi:o:")
    except getopt.GetoptError:
        print 'BoxSpider.py -s <season> -n <num_games> -t <table>'

    season = None
    num_games = 100
    from_table = 'box'
    for opt, arg in opts:
        if opt == '-s':
            season = int(arg)
        elif opt == '-n':
            num_games = int(arg)
        elif opt == '-t':
            from_table = str(arg)

    return season, num_games, from_table

class BoxSpider(scrapy.Spider):
    name = "BoxSpider"
    allowed_domains = ["stats.ncaa.org"]
    # season, num_games, from_table = parse_args(sys.argv[1:])
    start_urls = dbutil.get_games_to_scrape(season=2016, from_table='box', link_type='box', num_games=1000)
    print(len(start_urls))

    def __init__(self):
        self.data = []
        self.failed_urls = []
        self.items = []
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def parse(self, response):
        if response.status == 404:
            self.failed_urls.append(response.url)
            print(response.url, "failed")
        try:
            soup = BeautifulSoup(response.body, 'html.parser')
            header_table, box_stats = BoxScraper.extract_box_stats(soup, response.url)
            if BoxScraper.is_valid_stats(box_stats):
                dbutil.insert_box_stats(box_stats)
        except:
            traceback.print_exc()

    def spider_closed(spider):
        """Activates on spider closed signal"""
        spider.crawler.stats.set_value('failed_urls', ','.join(spider.failed_urls))



if __name__ == "__main__":
    spider = BoxSpider()
    settings = Settings()
    settings.set('DOWNLOAD_DELAY', 0.5)
    settings.set('COOKIES_ENABLED', False)

    crawler = Crawler(spider, settings)
    crawler.crawl()
    print "______"
    # stop reactor when spider closes
    # crawler.signals.connect(spider_closing, signal=signals.spider_closed)
    reactor.run()
    print crawler.stats.get_stats()

