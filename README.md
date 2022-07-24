# Scrape macro data
Scrape macro data quickly and easily in python, asynchronously!

These tools are meant to provide users with programatic access to financial data sources. I put the effort in to scrape these so that you don't have to.

Currently only CME SOFR futures are supported, but its pretty easy to add other CME sources. If you find the product code and the landing page, you can build a class in like 2 lines that scrapes a different futures product from the site.

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
