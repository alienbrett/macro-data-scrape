import asyncio
import httpx
import pandas as pd
import dataclasses
import typing
import math


user_agent = "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.134 Mobile Safari/537.36"
headers = {
    'User-Agent': user_agent,
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Connection': 'keep-alive',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US;q=0.9',
    'Upgrade-Insecure-Requests': '1',
}

@dataclasses.dataclass
class CMEFutureScrapeRequest:
    page_url    : str
    product_id  : int
    data_url    : str = 'https://www.cmegroup.com/CmeWS/mvc/Quotes/Future/{0}/G'
    data_df     : typing.Optional[pd.DataFrame] = None
    timeout     : float = 5

    def get_time(self):
        return math.floor(time.time())

    def resolve_data_url(self,):
        return self.data_url.format(self.product_id)

    def process_data(self, raw_data : typing.Dict) -> pd.DataFrame:
        dj = pd.DataFrame(raw_data)
        df = dj.drop(columns=['quotes']).merge(dj['quotes'].apply(pd.Series), left_index=True, right_index=True)
        return df

    async def load(self, verbose=False):
        if self.data_df is not None:
            pass

        async with httpx.AsyncClient(headers=headers, timeout=self.timeout) as client:
            page = await client.get(self.page_url, )
            page.raise_for_status()

            if verbose:
                print('loaded main page')

            params = {
                '_t': self.get_time(),
            }

            page = await client.get(self.resolve_data_url(), headers = {'Referer':self.page_url}, params=params)
            page.raise_for_status()

            raw_data = page.json()
            if verbose:
                print('loaded quotes page')
            self.data_df = self.process_data(raw_data)
            if verbose:
                print('found {0} quotes'.format(len(self.data_df)))



@dataclasses.dataclass
class CME3MSOFRFutureScrapeRequest(CMEFutureScrapeRequest):
    page_url    : str = 'https://www.cmegroup.com/markets/interest-rates/stirs/three-month-sofr.quotes.html'
    product_id  : int = 8462


@dataclasses.dataclass
class CME1MSOFRFutureScrapeRequest(CMEFutureScrapeRequest):
    page_url    : str = 'https://www.cmegroup.com/markets/interest-rates/stirs/one-month-sofr.quotes.html'
    product_id  : int = 8463
