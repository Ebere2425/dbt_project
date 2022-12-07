/* There are no dupliactes present in the table so we can expect 
that the valuie will be unique to a country after filtering by year
This code filters the table by the required year and incicator_code 
and takes the MAX value of the result and the Gap btw both values 
*/
SELECT country_name
 , country_code
 , indicator_name
 , MAX(value) OVER(PARTITION BY year) as highest_value
 , value AS country_value
 , MAX(value) OVER(PARTITION BY year) - value AS Gap
 FROM `bigquery-public-data.world_bank_health_population.health_nutrition_population` 
 WHERE year = 2018 and indicator_code = "SH.XPD.CHEX.PP.CD"
 
