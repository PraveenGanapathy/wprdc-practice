SELECT
    municipality,
    EXTRACT(YEAR FROM sale_date)::int AS sale_year,
    COUNT(*) AS sales,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median_price
FROM {{ ref('stg_sales') }}
WHERE saledesc = 'VALID SALE'
  AND price > 0
   AND sale_date IS NOT NULL
GROUP BY municipality, sale_year