# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
# noinspection PyBroadException
import json
from abc import ABC, abstractmethod
from collections import OrderedDict
from datetime import datetime as dttm

import pandas as pd
import pyodbc
import requests
# from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from exchange_mapping import exchange_mapping


def ping() -> object:
    # contents = urllib.request.urlopen("https://api.coingecko.com/api/v3/ping").read()
    response = requests.get('https://api.coingecko.com/api/v3/coins/list', json={"include_platform": True})
    print(response.json())
    return None


#
# create table crypto_curr (
# cob date,
# symbol varchar(30),
# name varchar(200),
# exchange_id varchar(30),
# price float,
# max_supply float,
# circulating_supply float,
# total_supply float,
# volume_24h float,
# volume_change_24h float,
# market_cap float,
# fully_diluted_market_cap float,
# coingecko_mktcap_rank float,
# cmc_rank float,
# market_cap_dominance float,
# adoption int
# )
# CREATE UNIQUE INDEX crypto_curr_index ON  crypto_curr(cob,symbol) WITH IGNORE_DUP_KEY

class MarketDataIntfc(ABC):
    @abstractmethod
    def get_symbol_data(self) -> pd.DataFrame:
        return pd.DataFrame()

    @abstractmethod
    def get_exchange_data(self) -> pd.DataFrame:
        return pd.DataFrame()

    @abstractmethod
    def get_request_data(self, url) -> list:
        return []


class CoinMarketCapAPI(MarketDataIntfc):
    def __init__(self, apikey):
        self.apikey = apikey

    def get_request_data(self, url) -> list:
        parameters = {
            'start': '1',
            'limit': '5000'
        }
        headers = {
            'Accepts': 'application/json',
            'X-CMC_PRO_API_KEY': self.apikey,
        }

        try:
            session = requests.Session()
            session.headers.update(headers)
            response = session.get(url, params=parameters)
            data = json.loads(response.text)
            return data
        except (ConnectionError, Timeout, TooManyRedirects) as e:
            print(e)
            return []

    def get_symbol_data(self) -> pd.DataFrame:
        url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
        crypto_list_with_dups = self.get_request_data(url)
        if crypto_list_with_dups:
            if crypto_list_with_dups['status']['error_code'] != 0:
                print(crypto_list_with_dups['status']['error_code'].error_message)
                return pd.DataFrame()
        else:
            print("No data")
            return pd.DataFrame()

        crypto_list_with_dups = crypto_list_with_dups['data']
        symbol_vs_vol = {}
        for crypto in crypto_list_with_dups:
            if not crypto['symbol']: continue
            volume_24h = crypto['quote'].get('USD', {'volume_24h': 0})['volume_24h']
            if not volume_24h: continue
            if not crypto['symbol'] in symbol_vs_vol or \
                    symbol_vs_vol[crypto['symbol']] < volume_24h:
                symbol_vs_vol[crypto['symbol']] = volume_24h

        inserted_symbol = set()
        crypto_list = []
        for crypto in crypto_list_with_dups:
            if not crypto.get('symbol', None): continue
            volume_24h = crypto['quote'].get('USD', {'volume_24h': 0})['volume_24h']
            if not volume_24h: continue
            if crypto['symbol'] in inserted_symbol: continue
            if volume_24h >= symbol_vs_vol[crypto['symbol']]:
                crypto_list.append(crypto)
                inserted_symbol.add(crypto['symbol'])

        try:
            d = OrderedDict()
            d['symbol_data_source'] = ['COINMARKETCAPAPI'] * len(crypto_list)
            d['coin_mktcap_id'] \
                = [crypto_row['id'] for crypto_row in crypto_list]
            d['name'] = [crypto_row['name'] for crypto_row in crypto_list]
            d['symbol'] = [crypto_row['symbol'].upper() for crypto_row in crypto_list]
            d['adoption'] = [len(crypto_row.get('tags', [])) for crypto_row in crypto_list]
            d['max_supply'] = [crypto_row['max_supply'] for crypto_row in crypto_list]
            d['circulating_supply'] = [crypto_row['circulating_supply'] for crypto_row in crypto_list]
            d['total_supply'] = [crypto_row['total_supply'] for crypto_row in crypto_list]
            d['cmc_rank'] = [crypto_row['cmc_rank'] for crypto_row in crypto_list]
            d['price'] = [crypto_row['quote'].get('USD', {'price': 0})['price'] for crypto_row in crypto_list]
            d['volume_24h'] = [crypto_row['quote'].get('USD', {'volume_24h': 0})['volume_24h'] for
                               crypto_row in crypto_list]
            d['volume_change_24h'] = [
                0.01 * crypto_row['quote'].get('USD', {'volume_change_24h': 0})['volume_change_24h']
                for crypto_row in crypto_list]
            d['market_cap'] = [crypto_row['quote'].get('USD', {'market_cap': 0})['market_cap']
                               for crypto_row in crypto_list]
            d['market_cap_dominance'] = \
                [0.01 * crypto_row['quote'].get('USD', {'market_cap_dominance': 0})['market_cap_dominance']
                 for crypto_row in crypto_list]
            d['fully_diluted_market_cap'] = \
                [crypto_row['quote'].get('USD', {'fully_diluted_market_cap': 0})['fully_diluted_market_cap']
                 for crypto_row in crypto_list]
            df = pd.DataFrame(d)
            # df=df.set_index(['symbol'])
        except Exception as exc:
            print(exc)
            return pd.DataFrame()
        return df

    def get_exchange_data(self) -> pd.DataFrame:
        # Not allowed in basic plan
        d = pd.DataFrame()
        return d


class CoinAPI(MarketDataIntfc):
    def __init__(self, apikey):
        self.apikey = apikey

    def get_request_data(self, url) -> list:
        headers = {
            'Accepts': 'application/json',
            'X-CoinAPI-Key': self.apikey,
        }
        try:
            session = requests.Session()
            session.headers.update(headers)
            response = session.get(url)
            data = json.loads(response.text)
            spot_data = []
            for elem in data:
                if elem['symbol_type'].upper() == 'SPOT': spot_data.append(elem)
        except (ConnectionError, Timeout, TooManyRedirects) as e:
            print(e)
            return []
        return spot_data

    def get_symbol_data(self) -> pd.DataFrame:
        url = 'https://rest.coinapi.io/v1/symbols'
        crypto_list_with_dups = self.get_request_data(url)
        symbol_vs_vol = {}
        crypto_list = []
        for crypto in crypto_list_with_dups:
            if not crypto.get('asset_id_base', None): continue
            if not crypto.get('volume_1day_usd', 0): continue
            if not crypto['asset_id_base'] in symbol_vs_vol or \
                    symbol_vs_vol[crypto['asset_id_base']] < crypto.get('volume_1day_usd', 0):
                symbol_vs_vol[crypto['asset_id_base']] = crypto.get('volume_1day_usd', 0)

        inserted_symbol = set()
        for crypto in crypto_list_with_dups:
            if not crypto.get('asset_id_base', None): continue
            if not crypto.get('volume_1day_usd', 0): continue
            if crypto['asset_id_base'] in inserted_symbol: continue
            if crypto.get('volume_1day_usd', 0) >= symbol_vs_vol[crypto['asset_id_base']]:
                crypto_list.append(crypto)
                inserted_symbol.add(crypto['asset_id_base'])

        d = OrderedDict()
        try:
            d['exchange_data_source'] = ['COINAPI'] * len(crypto_list)
            d['coin_api_id'] \
                = [crypto_row.get('symbol_id', '') for crypto_row in crypto_list]
            d['symbol'] = [crypto_row.get('asset_id_base', '').upper() for crypto_row in crypto_list]
            d['exchange_id'] = [crypto_row.get('exchange_id', '') for crypto_row in crypto_list]
            df = pd.DataFrame(d)
            # df = df.set_index('symbol')
        except Exception as exc:
            print(exc)
            return pd.DataFrame()
        return df

    def get_exchange_data(self) -> pd.DataFrame:
        return pd.DataFrame()  # No useful info here


class CoingeckoAPI(MarketDataIntfc):

    def get_request_data(self, url) -> list:
        try:
            response = requests.get(url)
            return response.json()
        except Exception as exc:
            print(str(exc))
            return []

    def get_symbol_data(self) -> pd.DataFrame:
        page_no = 1

        crypto_map = OrderedDict()
        while page_no < 100:
            print("Page Number =" + str(page_no))
            url = \
                'https://api.coingecko.com/api/v3/coins/markets?vs_currency=USD&order=market_cap_desc&per_page=1000&page=' + \
                str(page_no) + '&sparkline=false'
            crypto_list_page = self.get_request_data(url)
            if crypto_list_page:
                for crypto in crypto_list_page:
                    if not crypto.get('symbol', None):
                        continue
                    symbol = crypto['symbol']
                    if not crypto.get('total_volume', 0):
                        continue
                    if not crypto.get('market_cap', 0):
                        continue
                    volume = crypto.get('total_volume',0)
                    if symbol not in crypto_map or volume > crypto_map[symbol]['total_volume']:
                        crypto_map[symbol] = crypto
                page_no = page_no + 1
            else:
                break
        d = OrderedDict()
        try:
            d['symbol_data_source'] = ['COINGECKOAPI'] * len(crypto_map)
            d['coin_gecko_id'] \
                = [crypto_row['id'] for crypto_row in crypto_map.values()]
            d['name'] = [crypto_row.get('name', '') for crypto_row in crypto_map.values()]
            d['coingecko_mktcap_rank'] = [crypto_row.get('market_cap_rank', 0) for crypto_row in crypto_map.values()]
            d['symbol'] = [crypto_row['symbol'].upper() for crypto_row in crypto_map.values()]
            d['max_supply'] = [crypto_row.get('max_supply', 0) for crypto_row in crypto_map.values()]
            d['circulating_supply'] = [crypto_row.get('circulating_supply', 0) for crypto_row in crypto_map.values()]
            d['total_supply'] = [crypto_row.get('total_supply', 0) for crypto_row in crypto_map.values()]
            d['price'] = [crypto_row.get('current_price', 0) for crypto_row in crypto_map.values()]
            d['volume_24h'] = [crypto_row.get('total_volume', 0) for crypto_row in crypto_map.values()]  # WHAT IS THIS ?
            d['market_cap'] = [crypto_row.get('market_cap', 0) for crypto_row in crypto_map.values()]
            d['fully_diluted_market_cap'] = [crypto_row.get('fully_diluted_valuation', 0) for crypto_row in crypto_map.values()]
            df = pd.DataFrame(d)
            # df = df.set_index('symbol')
        except Exception as exc:
            print(str(exc))
            return pd.DataFrame()
        return df

    def convert_to_coin_format(self, coingecko_exchange_id: str):
        return coingecko_exchange_id.upper().replace("_", "")

    def get_exchange_data(self) -> pd.DataFrame:
        page_no = 1
        exchange_list = []
        inserted_exchanges = set()
        while page_no < 100:
            print("Page Number =" + str(page_no))
            url = 'https://api.coingecko.com/api/v3/exchanges?per_page=1000&page=' + str(page_no)
            exchange_list_page = self.get_request_data(url)
            if exchange_list_page:
                for entry in exchange_list_page:
                    if entry['id'] in inserted_exchanges: continue
                    if entry.get('trust_score', None): exchange_list.append(entry)
                page_no = page_no + 1
            else:
                break
        d = OrderedDict()
        try:
            d['exchange_data_source'] = ['COINGECKOAPI'] * len(exchange_list)
            d['id'] = [
                exchange_mapping.get(self.convert_to_coin_format(crypto_row.get('id', '')), crypto_row.get('id', ''))
                for crypto_row in exchange_list]
            d['exchange_name'] = [crypto_row.get('name', '') for crypto_row in exchange_list]
            d['trust_score'] = [crypto_row.get('trust_score', 0) for crypto_row in exchange_list]
            d['trust_score_rank'] = [crypto_row['trust_score_rank'] for crypto_row in exchange_list]
            d['trade_volume_24h_btc_normalized'] = [crypto_row.get('trade_volume_24h_btc_normalized', 0) for crypto_row
                                                    in exchange_list]
            df = pd.DataFrame(d)
        except Exception as exc:
            print(str(exc))

            return pd.DataFrame()
        return df


def write_df(flds, df2, tabname):
    server = 'localhost'  # to specify an alternate port
    database = 'master'
    now = dttm.now()
    df = df2.astype(object).where(pd.notnull(df2), None)
    row_no = 1
    try:
        c = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';Trusted_Connection=yes')
        cursor = c.cursor()
        sql = "insert into " + tabname + " ( cob, " + ",".join(flds) + " ) values ( " + "?," * (len(flds)) + " ? )"
        cursor.execute("SET ANSI_WARNINGS OFF")
        for index, row in df.iterrows():
            print("Inserting row No " + str(row_no))
            cursor.execute(sql, tuple([now.date()] + [row[field] for field in flds]))
            row_no = row_no + 1
        # commit the transaction
        c.commit()
    except Exception as ex:
        print(str(ex))

    # Things to capture
    # Read https://www.finextra.com/blogposting/20638/understanding-tokenomics-the-real-value-of-crypto
    # Allocation -- whether pre-minted ( eg before going live allocated to exclusive community
    #            -- or fairly launched
    # Supply of tokens ( max supply = constant -> deflationary, otherwise inflationary
    #            -- gradually growing supply is good
    # distribution -- is the token concentrated among only a few wallets
    #

    # basic symbol data from COINGECKO
    # augment with symbol <-> exchangeid info from COINAPI
    # get exchange details from coingecko
    # augment with 'cmc_rank', 'market_cap_dominance , adoption from coinmarketcapapi
    # make sure market cap ,volume are in USD from gecko

    # write_df(fields, crypto_table)


def extract(tokens):
    coingecko = CoingeckoAPI()
    coin = CoinAPI(tokens["X-CoinAPI-Key"])
    coinmarketcap = CoinMarketCapAPI(tokens["X-CMC_PRO_API_KEY"])

    # Exchange details , the coingecko ids need to be converted to coin format
    exchange_info = coingecko.get_exchange_data()
    write_df(['exchange_data_source', 'id', 'trust_score', 'trust_score_rank', 'trade_volume_24h_btc_normalized'],
             exchange_info, "dbo.crypto_exchange")

    # Core symbol data
    symbols_df_cgko = coingecko.get_symbol_data().rename(columns={'coin_gecko_id': 'symbol_data_source_id'})

    # symbol  vs exchange mapping
    symbols_df_coin = coin.get_symbol_data()

    # some auxiliary data
    symbols_df_cmc = coinmarketcap.get_symbol_data().filter([
        'symbol',
        'symbol_data_source',
        'adoption',
        'coin_mktcap_id',
        'cmc_rank',
        'market_cap_dominance',
        'volume_change_24h']).rename(columns={'symbol_data_source': 'additional_data_source'})

    # join symbol data from coingecko with symbol data from coin api to get exchange_id from coin api
    symbols_df = pd.merge(left=symbols_df_cgko, right=symbols_df_coin, how='left', left_on='symbol', right_on='symbol')
    flds = [
        'symbol',
        'name',
        'exchange_id',
        'price',
        'max_supply',
        'circulating_supply',
        'total_supply',
        'volume_24h',
        'volume_change_24h',
        'market_cap',
        'fully_diluted_market_cap',
        'coingecko_mktcap_rank',
        'cmc_rank',
        'market_cap_dominance',
        'adoption']

    # join with coin market cap data to get some auxiliary info
    symbols_df = pd.merge(left=symbols_df, right=symbols_df_cmc, how='left', left_on='symbol', right_on='symbol')[flds]

    # symbols_df.to_csv("C:\\Users\\jaydi\\Documents\\testdata\\symbols_and_exchange.csv", index=False)
    write_df(flds, symbols_df, "dbo.crypto_curr")

    print("ALL DONE with save")

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
