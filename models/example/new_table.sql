{{ config(materialized='table',
tags=['daily']) }}

/* This Model sources data from the population_by_country staging table and transposes the columns in it.
It uses the fhoffa.x.unpivoit() function to achieve this.
The parameters of the function are the table name and a regex of the columns top be transposed.
It returns a key value pair data structure of the original data. 
*/
WITH base AS (
    SELECT country
    , country_code
    , unpivotted
    FROM {{ ref('population_by_country') }} a
    --pass the table name and "year" as it is seen first in all the tables to be transposed. 
  , UNNEST(fhoffa.x.unpivot(a, 'year')) unpivotted
)

SELECT country
, country_code 
--unpivot the returned data in the first CTE
,CAST (RIGHT(unpivotted.key,4) AS INT64) AS as_of_year
,unpivotted.value AS population 
FROM base 
--filters data to meet criteria 
WHERE CAST (RIGHT(unpivotted.key,4) AS INT64)  >= 2010

