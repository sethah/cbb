import csv
import os

import scrapy
from bs4 import BeautifulSoup
import psycopg2
from twisted.internet import reactor

print os.environ['PYTHONPATH'].split(os.pathsep)
from DataCollection.Crawlers.stats.ScrapeUtils import ScheduleScraper
import DataCollection.DBScrapeUtils as dbutil
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from scrapy import signals
conn = psycopg2.connect(database="cbb", user="sethhendrickson",
                        password="abc123", host="localhost", port="5432")

cur = conn.cursor()
q = """SELECT ncaaid FROM teams WHERE ncaaid IS NOT NULL"""
cur.execute(q)
results = cur.fetchall()
teams = [result[0] for result in results]
urls = []
for year in range(2009, 2015):
    year_code = ScheduleScraper.convert_ncaa_year_code(year)
    urls += ['http://stats.ncaa.org/team/index/%s?org_id=%s' % (year_code, team) for team in teams]
# print urls
# urls = ['http://stats.ncaa.org/team/index/10440?org_id=167']

class ScheduleSpider(scrapy.Spider):
    name = "schedule"
    allowed_domains = ["stats.ncaa.org"]
    start_urls = urls

    def __init__(self):
        self.data = []
        self.failed_urls = []
        self.items = []

    def parse(self, response):
        if response.status == 404:
            self.failed_urls.append(response.url)
            print '****************'
            print response.url
        filename = 'test.txt'
        soup = BeautifulSoup(response.body, 'html.parser')
        games = ScheduleScraper.get_team_schedule(soup, response.url)
        team_id, year = ScheduleScraper.url_to_teamid(response.url)
        item = ScheduleItem()
        item['games'] = games
        self.items.append(item)

    def handle_spider_closed(self, spider, reason):
        self.crawler.stats.set_value('failed_urls', ','.join(spider.failed_urls))
        self.crawler.stats.set_value('seth', 'asdfasdfasdf')

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
    crawler = Crawler(spider, Settings())
    crawler.crawl()
    # stop reactor when spider closes
    crawler.signals.connect(spider_closing, signal=signals.spider_closed)
    reactor.run()
    print crawler.stats.get_stats()
    dbutil.update_games_table()
    os.remove("output.csv")

