
import sqlite3
import time

import numpy as np
import requests
import json

from datetime import datetime
from bs4 import BeautifulSoup

con = sqlite3.connect('database.db')
cur = con.cursor()

debug = True

# keywords -> words that will be searched
# excluded words -> some words can confuse our bot
queries = [
        {'5-5600':
            {'keywords': ['5600', 'amd'],
             'excluded_keywords': ['5600x','5600g', '4060ti'],
             'extra': '?page_size=100&facet_filters=eyJwcmljZSI6eyJtaW4iOjYwMCwibWF4Ijo2OTI3Ni43MX19&sort=price&variant=catalog' }
        },
        {'rx7600':
            {'keywords': ['rx7600'],
             'excluded_keywords': [],
             'extra': '?page_size=100&facet_filters=eyJwcmljZSI6eyJtaW4iOjYwMCwibWF4Ijo2OTI3Ni43MX19&sort=price&variant=catalog' }
        },
        {'rx-7600':
            {'keywords': ['rx7600'],
             'excluded_keywords': ['xt', 'pc'],
             'extra': '?page_size=100&facet_filters=eyJwcmljZSI6eyJtaW4iOjYwMCwibWF4Ijo2OTI3Ni43MX19&sort=price&variant=catalog' }
        },
        {'b550m':
            {'keywords': ['b550m', 'msi'],
             'excluded_keywords': ['kit'],
             'extra': '?page_size=100&facet_filters=eyJwcmljZSI6eyJtaW4iOjYwMCwibWF4Ijo2OTI3Ni43MX19&sort=price&variant=catalog' }
        },
        {'b550m':
            {'keywords': ['b550m', 'aorus'],
             'excluded_keywords': ['kit'],
             'extra': '?page_size=100&facet_filters=eyJwcmljZSI6eyJtaW4iOjYwMCwibWF4Ijo2OTI3Ni43MX19&sort=price&variant=catalog' }
        },
        {'rtx-4060':
            {'keywords': ['rtx4060', 'geforce'],
             'excluded_keywords': ['ti'],
             'extra': '?page_size=100&facet_filters=eyJwcmljZSI6eyJtaW4iOjYwMCwibWF4Ijo2OTI3Ni43MX19&sort=price&variant=catalog' }
        },
        {'550w':
            {'keywords': ['550', 'mwe', 'coolermaster'],
             'excluded_keywords': [],
             'extra': '?page_number=1&page_size=100&facet_filters=eyJwcmljZSI6eyJtaW4iOjI1MCwibWF4Ijo4NjA0MH19&sort=price&variant=catalog'}
        },
        {'550w':
            {'keywords': ['550', 'cyclops', 'gamdias'],
             'excluded_keywords': [],
             'extra': '?page_number=1&page_size=100&facet_filters=eyJwcmljZSI6eyJtaW4iOjI1MCwibWF4Ijo4NjA0MH19&sort=price&variant=catalog' }
        },
        {'550w':
            {'keywords': ['550', 'cv', 'corsair'],
             'excluded_keywords': [],
             'extra': '?page_number=1&page_size=100&facet_filters=eyJwcmljZSI6eyJtaW4iOjI1MCwibWF4Ijo4NjA0MH19&sort=price&variant=catalog'}
        }]

def get_best_price(item):
    arr = np.array([float(item['price']), \
            #float(item['primePrice']), 
            float(item['priceWithDiscount'])])

    if item['offer']:
        arr = np.append(arr, float(item['offer']['priceWithDiscount']))
        arr = np.append(arr, float(item['offer']['price']))

    if debug:
        print("Comparing: {}".format(arr))

    return str(np.min(arr[np.nonzero(arr)]))

def send_request_and_parse_items(url):
    while True:
        try:
            response = requests.get(url)

            if response.status_code == 200:
                content = response.text
                soup = BeautifulSoup(content, 'html.parser')
                res = soup.find(id="__NEXT_DATA__").text

                return True, json.loads(res)['props']['pageProps']['data']['catalogServer']['data']
            #else:
            #    return False, f'Error STATUS CODE: {response.status_code}'
        except:
            print("Cant connect to socket. Retrying...")

def valid_item(item, restriction):
    f_check = True
    s_check = True
    item_name = item['name'].strip().replace(" ", "").lower()

    # first check
    for keyword in restriction['keywords']:
        if keyword not in item_name:
            f_check = False
            break

    # second check
    for keyword in restriction['excluded_keywords']:
        if keyword in item_name:
            s_check = False
            break

    return f_check and s_check

def scrape_page(url, restriction):
    item_obj = []

    status, res = send_request_and_parse_items(url)

    # error when trying to retrieve page
    if not status:
        print("Status code error", res)
        return

    if debug:
        print(f'Status: {status}')
    for item in res:
        if valid_item(item, restriction):
            item_obj.append({
                'code': item['code'],
                'name': item['name'],
                'price': float(get_best_price(item))
            })

    # returns list sorted by best price
    return sorted(item_obj, key=lambda d: d['price'])

def check_prices(data):
    for d in data:
        res = query_database('select', f"SELECT code, price FROM main_table WHERE code = {d['code']}", None)
        if res:
            code, price = res[0]
            if d['price'] < float(price):
                print("[ABAIXOU] Valor de {} foi alterado de {} para {}".format(d['name'], float(price), d['price']))
                cur.execute("INSERT INTO price_change_table VALUES (?, ?, ?, ?, ?)", (d['code'], d['name'], d['price'], str(datetime.today()), 'ABAIXOU'))
                con.commit()
            elif d['price'] > float(price):
                print("[AUMENTOU] Valor de {} foi alterado de {} para {}".format(d['name'], float(price), d['price']))
                cur.execute("INSERT INTO price_change_table VALUES (?, ?, ?, ?, ?)", (d['code'], d['name'], d['price'], str(datetime.today()), 'AUMENTOU'))
                con.commit()
            else:
                if debug:
                    print("[ESTABILIZOU] Valor de {} continua o mesmo: {} == {}".format(d['name'], float(price), d['price']))


def update_database(data):
    for d in data:
        res = query_database('select', f"SELECT * FROM main_table WHERE code = {d['code']}", None)

        # do not exist, insert new hardware into table
        if len(res) == 0:
            if debug:
                print("Inserting: {}".format(d))
            cur.execute("INSERT INTO main_table VALUES (?, ?, ?)", (d['code'], d['name'], d['price']))
            con.commit()
        else:
            name, code, price = res[0]
            if debug:
                print("Atualizando: {} [last: {}]".format(d, price))
            query_database('update', f"UPDATE main_table SET price = {d['price']} WHERE code = {d['code']}", None)

def search_items():
    for query in queries:
        query_search = list(query.keys())[0]

        url = 'https://www.kabum.com.br/busca/' + query_search + query[query_search]['extra']
                #'?page_size=100&facet_filters=eyJwcmljZSI6eyJtaW4iOjYwMCwibWF4Ijo2OTI3Ni43MX19&sort=price&variant=catalog'

        if debug:
            print(f"Query: {query}")
            print(f"URL: {url}")
            print(f"===============")
        res = scrape_page(url, query[query_search])
        check_prices(res)
        update_database(res)

        if debug:
            print(json.dumps(res, indent=4))

def query_database(op, query, params):
    res = cur.execute(query)

    if op == 'insert' or op == 'update':
        return con.commit()

    if op == 'select':
        return res.fetchall()

def setup_database():
    query_database('none', 'CREATE TABLE IF NOT EXISTS main_table (code, name, price)', None)
    query_database('none', 'CREATE TABLE IF NOT EXISTS price_change_table (code, name, price, date, label)', None)

def dump_database():
    res = query_database('select', f"SELECT * FROM main_table", None)
    for r in res:
        print(r)


if __name__ == '__main__':
    setup_database()

    while True:
        print("Mudanças encontradas:")
        search_items()
        time.sleep(1 * 60)
    con.close()

