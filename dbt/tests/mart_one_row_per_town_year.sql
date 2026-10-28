SELECT municipality, sale_year, count(*)
FROM {{ ref('mart_muni_year_prices') }}
GROUP BY municipality, sale_year
HAVING count(*) > 1