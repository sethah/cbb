import scrapy
from bs4 import BeautifulSoup

url = 'http://stats.ncaa.org/team/index/10440?org_id=649'

class StatsSpider(scrapy.Spider):
    name = 'stats'
    start_urls = [url]

    def parse(self, response):
        soup = BeautifulSoup(response.content, 'html.parser')
        yield 10