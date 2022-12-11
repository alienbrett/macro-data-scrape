import typing
import enum
import dataclasses
import httpx
import pandas as pd


class OISIndex(enum.Enum):
    sofr    = 'SOFR'
    effr    = 'FEDFUND'
    obfr    = 'OBFR'


_product_code_lookup = {
    OISIndex.sofr : '520',
    OISIndex.effr : '500',
    OISIndex.obfr : '505',
}


@dataclasses.dataclass
class FedOISResetHistoryRequest:
    index   : OISIndex
    limit   : int
    timeout : float = 5.0
    data_df : typing.Optional[pd.DataFrame] = None

    async def load(self, ) -> typing.Optional[pd.DataFrame]:
        '''Returns dataframe of historical OIS rates.
        Data can be viewed online at https://www.newyorkfed.org/markets/reference-rates/sofr, or .../effr for fed funds, etc
        '''
        product_code = _product_code_lookup[self.index]

        if self.data_df is None:
            url = 'https://markets.newyorkfed.org/read'
            params = {
                'productCode': '50',
                'eventCodes': product_code,
                'limit': self.limit,
                'startPosition': '0',
                'format': 'json',
                'sort': 'postDt:-1',
            }
            async with httpx.AsyncClient(timeout = self.timeout) as client:
                page = await client.get(url, params=params)
            page.raise_for_status()

            page_data = page.json()
            self.data_df = pd.DataFrame(page_data)['refRates'].apply(pd.Series).set_index('effectiveDate')

