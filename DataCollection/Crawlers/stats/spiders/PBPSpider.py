import scrapy
from bs4 import BeautifulSoup
from twisted.internet import reactor
import sys, getopt
import traceback

from DataCollection.ScrapeUtils import PBPScraper
import DataCollection.DBScrapeUtils as dbutil
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher

class BoxSpider(scrapy.Spider):
    name = "PBPSpider"
    allowed_domains = ["stats.ncaa.org"]
    # season, num_games, from_table = parse_args(sys.argv[1:])
    # start_urls = ['http://stats.ncaa.org/game/play_by_play/3965776']
    start_urls = dbutil.get_games_to_scrape(season=2016, from_table='raw_pbp', link_type='pbp', num_games=500)
    print(start_urls)

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
            # print(response.body)
            header_table, pbp_stats = PBPScraper.extract_pbp_stats(soup, response.url)
            # print(header_table.head(), pbp_stats.head(10))
            # print(pbp_stats.head())
            dbutil.insert_raw_pbp_data(pbp_stats.values)
            # if BoxScraper.is_valid_stats(box_stats):
            #     dbutil.insert_box_stats(box_stats)
        except:
            traceback.print_exc()

    def spider_closed(spider):
        """Activates on spider closed signal"""
        spider.crawler.stats.set_value('failed_urls', ','.join(spider.failed_urls))



if __name__ == "__main__":
    pass
    # spider = BoxSpider()
    # settings = Settings()
    # settings.set('DOWNLOAD_DELAY', 0.5)
    # settings.set('COOKIES_ENABLED', False)
    #
    # crawler = Crawler(spider, settings)
    # crawler.crawl()
    # print "______"
    # # stop reactor when spider closes
    # # crawler.signals.connect(spider_closing, signal=signals.spider_closed)
    # reactor.run()
    # print crawler.stats.get_stats()

