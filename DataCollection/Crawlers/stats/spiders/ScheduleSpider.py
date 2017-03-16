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
from scrapy.xlib.pydispatch import dispatcher

class ScheduleSpider(scrapy.Spider):
    name = "ScheduleSpider"
    allowed_domains = ["stats.ncaa.org"]
    # start_urls = ScheduleScraper.get_urls([2016])
    start_urls = ['http://stats.ncaa.org/team/141/12260']
    print(start_urls)

    def __init__(self):
        self.soups = []
        self.failed_urls = []
        self.games = []
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def parse(self, response):
        if response.status == 404:
            self.failed_urls.append(response.url)
            print('404 error: %s' % response.url)
        try:
            soup = BeautifulSoup(response.body, 'html.parser')
            games = ScheduleScraper.get_team_schedule(soup, response.url)
            # item = ScheduleItem()
            # item['games'] = games
            self.games.append(games)
            self.soups.append(str(soup))
        except Exception as e:
            # self.data.append("eee")
            print(e)

    def spider_closed(self, spider):
        """Activates on spider closed signal"""
        # log.msg("Closing reactor", level=log.INFO)
        spider.crawler.stats.set_value('failed_urls', ','.join(spider.failed_urls))
        print("a;sldkfja;slfdjds")
        # print(self.soups)

        text_file = open("/Users/sethhendrickson/cbbdb/soup.txt", "w")
        text_file.write(self.soups[0])
        text_file.close()

        with open("output.csv", "a") as f:
            writer = csv.writer(f)
            for games in self.games:
                writer.writerows(games)
        dbutil.update_games_table()
        os.remove("output.csv")

class ScheduleItem(scrapy.Item):
    games = scrapy.Field()



if __name__ == "__main__":
    spider = ScheduleSpider()
    settings = Settings()
    settings.set('DOWNLOAD_DELAY', 0.25)
    settings.set('COOKIES_ENABLED', False)
    crawler = Crawler(spider, settings)
    crawler.crawl()
    crawler.signals.connect(spider_closing, signal=signals.spider_closed)
    # reactor.run()
    print(crawler.stats.get_stats())
    dbutil.update_games_table()
    os.remove("output.csv")
    # reactor.stop()

