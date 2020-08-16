import re
import time
from datetime import datetime
from random import randint
from re import compile as recompile
from urllib.parse import quote

import pandas as pd
import requests
from bs4 import BeautifulSoup
from geopy.geocoders import Nominatim
import csv

today = datetime.today().date()
geolocator = Nominatim(user_agent='chulpan.zin@gmail.com')
headers = {"User-Agent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) '
                         'Chrome/84.0.4147.89 Safari/537.36'}


class TooManyRequests(Exception):
    """raises in fetch_page function if request redirected to https://www.avito.ru/blocked"""
    pass


def get_all_ads(query, sort_by='date', by_title=False, with_images=False, owner=None):
    """Yields dicts with ad info (title, link, price and date).

    Keyword arguments:
    query -- search query, like 'audi tt'
    sort_by -- method of sorting, 'date', 'price', 'price_desc' (price descending)
               default None (yields ads sorted by Avito algorithm)
    by_title -- if True yields only ads with query in title
                default False
    with_images -- if True yields only ads with query in title
                   default False
    owner -- if 'private' yields only private ads, if 'company' only company
             default None (yields all ads)
    """
    search_url = generate_search_url(query, sort_by, by_title, with_images, owner)
    for page, page_number in get_pages(search_url):
        for ad in get_ads_from_page(page):
            yield agregate_ad_info(ad, page_number)


def generate_search_url(query, sort_by, by_title, with_images, owner):
    """Generates url by search parametres
    raises ValueError if sort_by or owner argument is not correct
    """
    sort_values = {'date': '104', 'price': '1', 'price_desc': '2', None: '101'}
    owners = {'private': '1', 'company': '2', None: '0'}
    if sort_by not in sort_values:
        raise ValueError('Sorting by {} is not supported'.format(sort_by))
    if owner not in owners:
        raise ValueError('Owner can be only private or company')
    urlencoded_query = quote(query)
    return 'https://www.avito.ru/kazan?s={}&bt={}&q={}&i={}&user={}'.format(sort_values[sort_by],
                                                                            int(by_title),
                                                                            urlencoded_query,
                                                                            int(with_images),
                                                                            owners[owner]) + '&p={}'


def agregate_ad_info(ad, page_number):  # , info
    title = get_title(ad)
    link = get_link(ad)
    price, price_per_sqm = get_price(ad)

    closest_metro = get_metro(ad)
    metro_distance = get_metro_distance(ad)
    metro_distance_km = clean_metro_distance(metro_distance)

    date = get_current_date()
    publication_date = get_date(ad)
    publication_date = convert_date(publication_date)

    address = get_address(ad)
    address = clean_address(address)
    latitude, longitude = geocoding(address)
    district = get_district(latitude, longitude)

    return title, date, link, price, publication_date, \
           address, closest_metro, metro_distance_km, page_number, \
           latitude, longitude, district, price_per_sqm


def get_pages(search_url):
    """Yields page html as string until it reaches page with nothing found"""
    page_number = 1
    page = fetch_page(search_url.format(page_number))
    while (page_exists(page)) & (page_number <= 100):
        yield page, page_number
        page_number += 1
        page = fetch_page(search_url.format(page_number))


def get_ads_from_page(page):
    return get_beautiful_soup(page).find_all('div', attrs={'class': 'item_table-wrapper'})


def fetch_page(page_url):
    """Returns page html as string
    raises TooManyRequest if avito blocks IP
    """
    response = requests.get(page_url, headers=headers)  # define proxies if necessary

    if response.status_code == 429:
        raise TooManyRequests('IP temporarily blocked')
    time.sleep(randint(10, 18))
    return response.text


def get_beautiful_soup(html):
    return BeautifulSoup(html, 'html.parser')


def page_exists(page):
    return get_beautiful_soup(page).find('div', attrs={'class': 'item_table-wrapper'}) is not None


def get_title(ad):
    return ad.find('a', attrs={'class': 'snippet-link'})['title']


def get_link(ad):
    base_url = 'https://www.avito.ru'
    return base_url + ad.find('a', attrs={'class': 'snippet-link'})['href']


def get_price(ad):
    price = ad.find('div', attrs={'class': 'snippet-price-row'}).getText().replace('\n', '').split('₽')
    ppm = float(price[0].replace(' ', ''))
    per_sq_m = 'м²' in price[1]
    return ppm, per_sq_m


def get_date(ad):
    return ad.find('div', attrs={'class': 'snippet-date-info'})['data-tooltip']


def get_address(ad):
    return ad.find('span', attrs={'class': 'item-address__string'}).getText().strip()


def get_metro(ad):
    metro = ad.find('span', attrs={'class': 'item-address-georeferences-item__content'})
    return metro.getText().strip() if metro else None


def get_metro_distance(ad):
    distance = ad.find('span', attrs={'class': 'item-address-georeferences-item__after'})
    return distance.getText().strip().replace('\xa0', '') if distance else None


def get_current_date():
    return datetime.today().date()


def get_district(lat, long):
    try:
        loc = geolocator.reverse((lat, long))[0]
        district = re.match(".*,\s*(.*?) район.*", loc).group(1)
    except:
        district = None
    return district


def get_current_year():
    return datetime.today().year


def convert_date(date):
    months = {'января': '1', 'февраля': '2', 'марта': '3', 'апреля': '4', 'мая': '5', 'июня': '6',
              'июля': '7', 'августа': '8', 'сентября': '9', 'октября': '10', 'ноября': '11',
              'декабря': '12'}
    try:
        date_split = date.split()
    except:
        date_split = date
    date_split[1] = months[date_split[1]]
    publication_time = date_split[2]
    date_split[2] = str(get_current_year())
    date_converted = '-'.join(d for d in date_split[::-1])
    date_converted = ' '.join([date_converted, publication_time])
    date_converted = pd.to_datetime(date_converted)
    return date_converted


def clean_metro_distance(dist):
    if not dist:
        dist = '0км'
    return float(dist.replace('км', '').replace(',', '.')) if 'км' in \
                                                              str(dist) else float(dist.replace('м', '')) / 1000


def clean_address(address):
    rgx = recompile(r'[к](?=\d)')
    address = ''.join(['Татарстан, Казань, ', address])
    address = address.replace('пр-т', 'проспект')
    address = rgx.sub(', корпус ', address)
    return address


def geocoding(address):
    try:
        latitude = geolocator.geocode(address).latitude
        longitude = geolocator.geocode(address).longitude
    except:
        latitude, longitude = None, None
    return latitude, longitude


filename = 'avito_rentOffice{}.csv'.format(today)
rent_office_query = 'аренда помещений'

start = datetime.now()
print(start)
with open('rent_office/{}/{}'.format(today, filename), "a", encoding="utf-8-sig") as f:
    writes = csv.writer(f, delimiter=' ', quoting=csv.QUOTE_ALL)
    writes.writerows(get_all_ads(rent_office_query, sort_by='date'))
print(datetime.now() - start)
