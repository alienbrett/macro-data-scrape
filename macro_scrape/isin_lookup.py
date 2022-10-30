import asyncio
import json
import httpx
import urllib
import pandas as pd
import dataclasses
from bs4 import BeautifulSoup

from .headers import headers

from typing import Optional, List, Literal, Union


@dataclasses.dataclass
class SecuritySearchObj:
    isinType            : Literal['Primary Isin', ]     = 'Primary Isin'
    instrumentCategory  : Literal['01-All']             = '01-All'
    annaStatus          : Literal['1 All']              = '1 All'
    country             : Literal['All',]               = 'All'
    issueCurrency       : Literal['All',]               = 'All'
    entityNameOpton1    : Literal['1-ISSUER_NAMELONG',] = '1-ISSUER_NAMELONG'
    entityNameOpton2    : Literal['3 Contains',]        = '3 Contains'
    Search              : Literal['Search']             = 'Search'
    entityName          : Optional[str] = None
    isinValue           : Optional[str] = None
    rateFrom            : Optional[str] = None
    rateTo              : Optional[str] = None
    maturityFrom        : Optional[str] = None
    maturityTo          : Optional[str] = None
    updateFrom          : Optional[str] = None
    updateTo            : Optional[str] = None



base_url = 'https://www.annaservice.com'


@dataclasses.dataclass
class ISINSearchObj:
    '''Logs into Anna ISIN search service
    '''
    username    : str
    password    : str

    timeout     : float                         = 10.0
    client      : Optional[httpx.AsyncClient]   = None
    _csrf       : Optional[str]                 = None


    async def __aenter__(self,):
        self.client = await httpx.AsyncClient(headers=headers, timeout=self.timeout).__aenter__()
        await self.login()
        return self

    async def __aexit__(self, *args, **kwargs):
        await self.client.__aexit__(*args, **kwargs)


    @staticmethod
    def get_page_secret(text:str) -> str:
        soup = BeautifulSoup(text)
        id_tags = soup.findAll("input", {"type" : "hidden", 'name':'_csrf'})
        id_tag = id_tags[0]
        secret = id_tag['value']
        return secret


    async def login(self,):
        landing_url = urllib.parse.urljoin(base_url,'/isinlookup/login')
        login_url = urllib.parse.urljoin(base_url,'/isinlookup/authenticate')
        page = await self.client.get(landing_url)
        page.raise_for_status()
        login_data = {
            'username':self.username,
            'password':self.password,
            '_csrf': ISINSearchObj.get_page_secret(page.text),
        }

        page = await self.client.post(login_url, data=login_data, follow_redirects=True)
        page.raise_for_status()
        # Give the client the secret we were given
        self._csrf = ISINSearchObj.get_page_secret(page.text)
    

    async def search_securities(self, search_obj : SecuritySearchObj):
        search_url = urllib.parse.urljoin(base_url,'/isinlookup/search')

        data = dataclasses.asdict(search_obj)

        if self._csrf is None:
            page = await self.client.get(search_url, follow_redirects=True)
            page.raise_for_status()
            self._csrf = ISINSearchObj.get_page_secret(page.text)
        
        data['_csrf'] = self._csrf

        page = await self.client.post(search_url, data = data, follow_redirects=True)
        self._csrf = None
        page.raise_for_status()
        response = page.text.split('eval("results = " +')[1].split(';')[0][1:-2]
        response_js = json.loads(response)
        return pd.DataFrame(response_js['response']['isinRecords'])
