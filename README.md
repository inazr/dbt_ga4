# dbt_ga4

## info

author: me

license: MIT

## database

This dbt model for ga4 is currently designed to work with bigquery. I do not know if other db are fully functional as well. 

## concept

This models does not work with a persistent staging layer. Only the last 5 (last X) days will be loaded into stg_ga4 views to minimize the number of bytes processed and to speed up the model. The fct_ tables are using an incremental strategy to load all new rows from the stg_ layer.



