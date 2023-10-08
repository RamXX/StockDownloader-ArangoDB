# StockDownloader
Downloads the latest stock prices on an ArangoDB collection.

This program is similar to `assetdownloader` which uses Postgres as a DB. However, I've been doing a lot of work with ArangoDB recently, and I can benefit from having the same data set on Arango to model relationships among tickers.

I considered several approaches to this, but I opted for a single collection model as opposed to individual collections (akin to how `assetdownloader` uses tables for each ticker).

The initial insert process will be slow, largely due to the transformations needed for the dataframe, but subsequent processes as well as searches should be much faster. 

In any case, if performance is your goal for this type of application, Postgres will be a better alternative for this kind of dataset.