{{ config(materialized='table') }}

SELECT  * FROM
 `bigquery-public-data.world_bank_health_population.health_nutrition_population` 