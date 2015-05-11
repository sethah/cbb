import requests
from bs4 import BeautifulSoup
import re

def get_soup(url):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        return soup
    else:
        print 'Bad Status Code'
        return None


if __name__ == '__main__':
    # url = 'http://statsheet.com/mcb/games/scoreboard/2014-12-17'
    # soup = get_soup(url)
    # print soup
    r, c = (6, 3)
    n = 10*max((r+1)/2 - 1, 0)
    if r % 2 == 0:
        n += 1
    n += (c - 1)*2
    print n

