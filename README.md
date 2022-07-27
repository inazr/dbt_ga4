# dbt_ga4

## info

author: me

license: MIT

## database

This dbt models expects to be run on bigquery.

## concept

This models avoids a persistent staging layer. Only the last 5 (last X) days will be loaded into stg_ga4 views to minimize the number of bytes processed and to speed up the model. The downstream layers are using an incremental strategy to load all new rows from the stg_ layer.

## Good to know

The join key is hexed to prevent users from extracting information from compound keys. 



