import scrapy
from bs4 import BeautifulSoup
import psycopg2
import pandas as pd
from ..ScrapeUtils import ScheduleScraper

conn = psycopg2.connect(database="cbb", user="sethhendrickson",
                        password="abc123", host="localhost", port="5432")

cur = conn.cursor()
q = """SELECT ncaaid FROM teams LIMIT 350"""
cur.execute(q)
results = cur.fetchall()
teams = [result[0] for result in results]
urls = ['http://stats.ncaa.org/team/index/10440?org_id=%s' % team for team in teams]
urls = ['http://stats.ncaa.org/team/index/10440?org_id=167']

class ScheduleSpider(scrapy.Spider):
    name = "schedule"
    allowed_domains = ["stats.ncaa.org"]
    start_urls = urls

    def __init__(self):
        self.data = []
        self.failed_urls = []

    def parse(self, response):
        if response.status == 404:
            self.failed_urls.append(response.url)
        filename = 'test.txt'
        soup = BeautifulSoup(response.body, 'html.parser')
        games = ScheduleScraper.get_team_schedule(soup)
        # html_table = soup.findAll('table', {'class': 'mytable'})[0]
        # table = pd.read_html(str(html_table), header=0, skiprows=1)
        # print table[0].tail()
        # print str(html_table)
        self.data.append(games)
        # print '*'*10+str(games)+'*'*10
        # with open(filename, 'wb') as f:
        #     f.write(str(len(soup.findAll('a'))))

    def handle_spider_closed(self, spider, reason):
        self.crawler.stats.set_value('failed_urls', ','.join(spider.failed_urls))