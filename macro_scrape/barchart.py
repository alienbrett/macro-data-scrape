import logging
# import asyncio
import httpx
import datetime
import numpy as np
import pandas as pd
import dataclasses
import typing
import math
import time
import calendar
from typing import Optional, Literal
from io import StringIO
import urllib
import html5lib
from bs4 import BeautifulSoup
import pytz

from .headers import *

log = logging.getLogger(__name__)

cal = calendar.Calendar(firstweekday=calendar.SUNDAY)

us_central = pytz.timezone('US/Central')
us_eastern = pytz.timezone('US/Eastern')



def html_table_to_pandas(table_soup) -> pd.DataFrame:
    columns = []
    rows = []
    try:
        for row in table_soup.find('thead'):
            for cell in row.find_all('th'):
                columns.append(cell.text)
    except:
        columns = None
        pass
    
    for table_row in table_soup.find_all('tr'):
        rows.append([
            cell.text.strip()
            for cell in table_row.find_all('td')
        ])
    # print(rows)
    
    return pd.DataFrame(rows, columns=columns)





base_url = 'https://barchart.com/'

TickerKind = Literal[
    'futures',
]

@dataclasses.dataclass
class BCEODHistory:
    kind    : TickerKind
    ticker  : str

@dataclasses.dataclass
class BCFutureChainLookup:
    ticker : str



@dataclasses.dataclass
class BarchartSearchObj:
    timeout             : float                         = 10.0
    client              : Optional[httpx.AsyncClient]   = None
    _secret             : Optional[str]                 = None
    api_quote_url       : str                           = '/proxies/core-api/v1/quotes/get'
    api_min_quote_url   : str                           = '/proxies/timeseries/queryminutes.ashx'
    api_day_quote_url   : str                           = '/proxies/timeseries/queryeod.ashx'

    async def __aenter__(self,):
        self.client = await httpx.AsyncClient(headers=headers, timeout=self.timeout).__aenter__()
        return self

    async def __aexit__(self, *args, **kwargs):
        await self.client.__aexit__(*args, **kwargs)

    @staticmethod
    def to_numeric(xs: pd.Series):
        return pd.to_numeric(xs.str.replace('[^-.0-9]', ''))

    @staticmethod
    def to_date(xs: pd.Series):
        return pd.to_datetime(xs).dt.date
    
    @staticmethod
    def fmt_cols(
        df              : pd.DataFrame,
        date_cols       : typing.List[str] = [],
        numeric_cols    : typing.List[str] = [],
    ):
        for col in date_cols:
            if col in df.columns:
                df[col] = BarchartSearchObj.to_date(df[col])
        for col in numeric_cols:
            if col in df.columns:
                df[col] = BarchartSearchObj.to_numeric(df[col])
        return df

    def eat_secret_page_cookie(self, page) -> str:
        new_secret = page.cookies.get('XSRF-TOKEN')

        if new_secret is not None:
            self._secret = urllib.parse.unquote(new_secret)
            log.debug('got secret {0}'.format(self._secret))

        return self._secret


    async def perform_landing(self, relative_url:str):
        search_url = urllib.parse.urljoin(base_url, relative_url)
        log.debug('performing landing to url: {0}'.format(search_url))

        # Install the cookies & whatnot they give us
        page = await self.client.get(search_url, follow_redirects=True)
        page.raise_for_status()
        self.eat_secret_page_cookie(page)
        return page



    async def search_future(self, ticker : str):
        ticker = ticker.upper()
        futures_base = ticker[:-3]

        await self.perform_landing('/futures/quotes/{0}/overview'.format(ticker))

        search_url = urllib.parse.urljoin(base_url, self.api_quote_url)

        params = {
            'symbol': '{0}^F'.format(futures_base),
            'fields': 'symbol,symbolType,contractName,contractExpirationDate,lastPrice,priceChange,highPrice,lowPrice,volume,tradeTime,symbolCode',
        }

        page = await self.client.get(
            search_url,
            headers = {'x-xsrf-token':self._secret},
            params = params,
            follow_redirects=True,
        )
        page.raise_for_status()

        js = page.json()

        df = pd.DataFrame(js['data']).set_index('symbol')
        df = BarchartSearchObj.fmt_cols(
            df,
            date_cols       = ('contractExpirationDate', 'tradeTime'),
            numeric_cols    = ('volume',),
        )
        return df



    async def future_summary(self, ):
        await self.perform_landing('futures/major-commodities')
        search_url = urllib.parse.urljoin(base_url, self.api_quote_url)

        params = {
            'lists': 'futures.category.us.all',
            'fields': 'symbol,contractName,lastPrice,priceChange,openPrice,highPrice,lowPrice,volume,tradeTime,category,hasOptions,symbolCode,symbolType',
            'limit': '100',
            'meta': 'field.shortName,field.description,field.type,lists.lastUpdate',
            'groupBy': 'category',
            # 'raw': '1'
        }

        page = await self.client.get(
            search_url,
            headers = {'x-xsrf-token':self._secret},
            params = params,
            follow_redirects=True,
        )
        page.raise_for_status()

        js = page.json()

        dfs = []
        for k,v in js['data'].items():
            df = pd.DataFrame(v)
            df['group'] = k
            dfs.append(df)

        df = pd.concat(dfs, axis=0).set_index(['group','symbol'])
        df = BarchartSearchObj.fmt_cols(
            df,
            date_cols       = ('tradeTime',),
            numeric_cols    = ('volume',),
        )
        return df



    async def get_ohlc_quotes(self,
            ticker              : str,
            interval            : Optional[int],
            max_records         : int=10_000,
            dividends           : bool=False,
            back_adjust         : bool=False,
            days_to_expiration  : int = 1,
            contract_roll       : str = 'expiration',
        ):
        """Returns a dataframe of OHLC quotes for a given ticker.
        If interval is None, then it returns OHLC over each day. Otherwise, interval is the size in minutes of each OHLC interval.
        """
        ticker = ticker.upper()

        await self.perform_landing('/futures/quotes/{0}/overview'.format(ticker))

        if interval is None:
            search_url = urllib.parse.urljoin(base_url, self.api_day_quote_url)
        else:
            search_url = urllib.parse.urljoin(base_url, self.api_min_quote_url)

        params = {
            # 'fields': ','.join(fields),
            'symbol'            : ticker,
            'interval'          : str(interval),
            'maxrecords'        : str(max_records),
            'volume'            : 'contract',
            'order'             : 'asc',
            'dividends'         : str(dividends).lower(),
            'backadjust'        : str(back_adjust).lower(),
            'daystoexpiration'  : str(days_to_expiration),
            'contractroll'      : contract_roll,
        }

        page = await self.client.get(
            search_url,
            headers = {'x-xsrf-token':self._secret},
            params = params,
            follow_redirects=True,
        )
        page.raise_for_status()

        raw = StringIO(page.text)
        if interval is None:
            columns = [
                'open',
                'high',
                'low',
                'close',
                'volume',
                'open_interest',
            ]
        else:
            columns = [
                # 'timestamp',
                'something',
                'open',
                'high',
                'low',
                'close',
                'volume',
            ]
        df = pd.read_csv(raw, names=columns)
        if interval is None:
            df = df.loc[df.index.get_level_values(0)[0]]
            df.index = pd.to_datetime(df.index)
        else:
            df.index = pd.to_datetime(df.index).tz_localize(us_eastern)
        df = df.drop(columns={'something'}, errors='ignore')
        return df
    


    async def get_contract_details(self, future_ticker : str) -> pd.Series:

        landing_page = await self.perform_landing('futures/quotes/{0}/profile'.format(future_ticker.upper()))
        soup = BeautifulSoup(landing_page.text )
        contract_table_html = soup.find('div', {'class':'text-block futures'}).find('table')

        df = html_table_to_pandas(contract_table_html)
        df = df.set_index(df.columns[0])[df.columns[1]]
        return df
    


    async def get_all_historic_product_chain(self, product_ticker : str, page:int=1, limit:int=1_000) -> pd.DataFrame:
        await self.perform_landing('futures/major-commodities')
        search_url = urllib.parse.urljoin(base_url, self.api_quote_url)

        params = {
            # 'fields': 'contractExpirationDate.format(Y),symbol,contractNameHistorical,lastPrice,highPrice1y,highDate1y,lowPrice1y,lowDate1y,percentChange1y,tradeTime,symbolCode,symbolType,hasOptions',
            'fields': 'contractExpirationDate.format(Y),symbol,contractNameHistorical,symbolCode,symbolType,hasOptions',
            'list': 'futures.historical.byRoot({0})'.format(product_ticker),
            'orderBy': 'contractExpirationDate',
            'orderDir': 'asc',
            'meta': 'field.shortName,field.type,field.description,lists.lastUpdate',
            'hasOptions': 'true',
            'page': str(page),
            'limit': str(limit),
            'raw': '1',
        }

        page = await self.client.get(
            search_url,
            headers = {'x-xsrf-token':self._secret},
            params = params,
            follow_redirects=True,
        )
        page.raise_for_status()

        js = page.json()

        total_count = js['total']
        page_count = js['count']

        if total_count < page_count:
            return await self.get_all_historic_product_chain(product_ticker, page=1, limit=total_count)
        else:
            df = pd.DataFrame([row['raw'] for row in js['data']]).set_index('symbol')
            if 'tradeTime' in df.columns:
                df['tradeTime'] = pd.to_datetime(df['tradeTime'], unit='s').dt.tz_localize(pytz.utc).dt.tz_convert(us_eastern)
            return df


    async def get_all_historic_product_chain_quotes(self, product_ticker : str, page:int=1, limit:int=1_000) -> pd.DataFrame:
        all_contracts_df = await self.get_all_historic_product_chain(product_ticker, page=page, limit=limit)
        return all_contracts_df
