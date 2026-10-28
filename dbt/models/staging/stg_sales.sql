SELECT
    parid,
    TRIM(munidesc) AS municipality,
    municode,
    CAST(saledate AS date) AS sale_date,
    CAST(price AS numeric) AS price,
    saledesc,
    deedbook,
    deedpage
FROM {{ source('raw', 'sales') }}