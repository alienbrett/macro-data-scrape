# Scrape macro data
Scrape macro data quickly and easily in python, asynchronously!

These tools are meant to provide users with programatic access to financial data sources. I put the effort in to scrape these so that you don't have to.

## Supports
* SOFR historical rates, from NYC Fed site
* CME 1M and 3M SOFR futures quotes

Its pretty easy to add other CME sources if you need. If you find the product code and the landing page, you can build a class in like 2 lines that scrapes a different futures product from the site.

## Notice
* These endpoints may change, which could render these utils broken until fixed
* Data providers may delay the data feeds by a few minutes to hours, so there's no guarantee the data here is live

Use these tools at your own risk.


## Loading sofr futures data (1m + 3m futures)
```python3
import asyncio
from cme_scrape import CME1MSOFRFutureScrapeRequest, CME3MSOFRFutureScrapeRequest


sofr_1m_req = CME1MSOFRFutureScrapeRequest()
asyncio.run(sofr_1m_req.load(verbose=True))
print(sofr_1m_req.data_df.head())


sofr_3m_req = CME3MSOFRFutureScrapeRequest()
asyncio.run(sofr_3m_req.load(verbose=True))
print(sofr_3m_req.data_df.head())

```


## Loading sofr historical data
```python3
import asyncio
from fed_sofr_history import FedSOFRHistoryRequest

sofr_hist_req = FedSOFRHistoryRequest(limit=10)
asyncio.run(sofr_hist_req.load())
print(sofr_hist_req.data_df.head())

```

# Lookup ISIN numbers
Uses ANNA ISIN lookup service. Make an account over at [www.annaservice.com](https://www.annaservice.com/)
```python3

search_obj_1 = SecuritySearchObj(
    ### Lookup all entities with 'apple' in the description/name
    entityName = 'apple',
)

search_obj_2 = SecuritySearchObj(
    ### Get a security by ISIN directly
    isinValue = 'US0378331005',
)

async def main():
    async with ISINSearchObj(username='my_email@gmail.com', password='my_password') as client:
        df = await client.search_securities(search_obj = search_obj_1)
        print(df)

asyncio.run(main())
```

# NASDAQ stock listings
Download public Nasdaq FTP listing information. There are more options available than is displayed here.
```python3

import pytz

from macro_data_scrape.nasdaq import ftp as ndxftp

with fdxftp.get_nasdaq_ftp_client(
        ### Select one of the following to get a different dataset
        name = 'other_listed',
        # name = 'listed_tickers',
        # name = 'options',
        # name = 'bxoptions',
        # name = 'psx_traded',
    ) as client:

    print(client.last_modified().astimezone(pytz.timezone('US/Eastern')))

    if not client.local_copy_exists():
        print('downloading file')
        client.download_file()

    df = client.load_from_local()
print(df.iloc[[0]].T)
```
