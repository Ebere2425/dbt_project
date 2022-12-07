/* This Query takes data from two sources, 
Join the data to another table as well as does some calulation and cleaning on both data sets*/


WITH hnp AS (
SELECT 
country_code AS cc
, indicator_code
-- Takes care of the null values as well as changes the data type for calculation 
, COALESCE (CAST (value AS FLOAT64), 0)  AS hepc 
, year
 FROM {{ ref('health_nutrition_population') }}
where year >= 2010  and indicator_code = "SH.XPD.CHEX.PP.CD"

)

,base AS ( 
SELECT country 
,country_code
-- Gets rid of the null in the table, in this case the null value is written as a string 
,CASE WHEN population = 'null' THEN '0' ELSE population END AS population
,as_of_year
,COALESCE (hepc, 0) AS hepc
FROM {{ ref('new_table') }} a
LEFT JOIN hnp 
ON a.country_code = hnp.cc and a.as_of_year = hnp.year 
) 

, sub_res AS (
SELECT country 
, country_code
, as_of_year AS Year
, CAST (population AS FLOAT64) AS population
--Using the LAG function to pull the value & population of the previous year 
, COALESCE ( LAG (CAST (population AS FLOAT64), 1) OVER ( PARTITION BY country_code ORDER BY as_of_year ), 0)  AS prev_population
, hepc 
, COALESCE (LAG (hepc, 1 ) OVER ( PARTITION BY  country_code ORDER BY  as_of_year ), 0)   AS prev_h_e_p_c
FROM
base
ORDER BY country_code, as_of_year
)

SELECT 
country
, country_code
, year
, population
, ROUND (population/ NULLIF(prev_population,0), 2) AS change_in_health_expenditure_per_capita
, hepc as health_expenditure_per_capita
, ROUND (hepc /NULLIF(prev_h_e_p_c,0), 2)  AS change_in_population
 FROM 
sub_res