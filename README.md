### Description
* Flask applet for processing XML product catalog running in GCP App Engine
* Download XML Document of products from given URL
* Process document into Python dicts
* Download existing list of products from BigQuery
* Post all new products (with no sku matching records from BQ table) to a given URL
* If greater than 200 new products, split into batches of 200 or less
* UPDATE BigQuery table with new products in each batch
* Runs on cron
