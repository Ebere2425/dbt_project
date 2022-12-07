{{ config(materialized='table') }}

SELECT  * FROM
 `bigquery-public-data.world_bank_global_population.population_by_country` 
