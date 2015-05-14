import eventlet
from eventlet.green import urllib2
import requests
import httplib
import urllib2
import cookielib
from bs4 import BeautifulSoup

urls = [
    "https://www.google.com/intl/en_ALL/images/logo.gif",
    "http://python.org/images/python-logo.gif",
    "http://us.i1.yimg.com/us.yimg.com/i/ww/beta/y3.gif",
]
# urls = ['http://stats.ncaa.org/game/box_score/2791733',
# 'http://stats.ncaa.org/game/box_score/2796475',
# 'http://stats.ncaa.org/game/box_score/2804673',
# 'http://stats.ncaa.org/game/box_score/2816396',
# 'http://stats.ncaa.org/game/box_score/2821073',
# 'http://stats.ncaa.org/game/box_score/2828074',
# 'http://stats.ncaa.org/game/box_score/2833673',
# 'http://stats.ncaa.org/game/box_score/2838440',
# 'http://stats.ncaa.org/game/box_score/2858846',
# 'http://stats.ncaa.org/game/box_score/2869060',
# 'http://stats.ncaa.org/game/box_score/2889513',
# 'http://stats.ncaa.org/game/box_score/2896186',
# 'http://stats.ncaa.org/game/box_score/2920712',
# 'http://stats.ncaa.org/game/box_score/2950034',
# 'http://stats.ncaa.org/game/box_score/2951075',
# 'http://stats.ncaa.org/game/box_score/2976456',
# 'http://stats.ncaa.org/game/box_score/2976454',
# 'http://stats.ncaa.org/game/box_score/3012013',
# 'http://stats.ncaa.org/game/box_score/3023634',
# 'http://stats.ncaa.org/game/box_score/3079114']

# po = Page_Opener()
cookies = {'__utma': '64585069.776744488.1431305770.1431541118.1431549457.9',
            '__utmb': '64585069.1.10.1431549457',
            '__utmc': '64585069',
            '__utmt': '1',
            '__utmv': '',
            '__utmv': '64585069.1431305770.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none)'}
cookiejar = cookielib.LWPCookieJar()
def fetch(url):
    print("opening", url)
    cj = cookielib.CookieJar()
    cj = cookielib.LWPCookieJar()
    opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
    
    agent = 'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; NeosBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)'
    headers = {'User-Agent': agent}
    request = urllib2.Request(url, headers=headers)
    response = opener.open(request)
    body = response.read()
    # body = urllib2.urlopen(url).read()
    print("done with", url)
    return url, body


pool = eventlet.GreenPool(200)
for url, body in pool.imap(fetch, urls):
    print("got body from", url, "of length", len(body))