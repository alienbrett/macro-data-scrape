# import asyncio
import httpx
import pandas as pd
import dataclasses
import typing


@dataclasses.dataclass
class FedSOFRHistoryRequest:
    limit   : int
    timeout : float = 5.0
    data_df : typing.Optional[pd.DataFrame] = None

    async def load(self, ) -> typing.Optional[pd.DataFrame]:
        '''Returns dataframe of historical sofr rates.
        Data can be viewed online at https://www.newyorkfed.org/markets/reference-rates/sofr
        '''

        if self.data_df is None:
            url = 'https://markets.newyorkfed.org/read'
            params = {
                'productCode': '50',
                'eventCodes': '520',
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

