SELECT municipality
FROM {{ ref('stg_sales') }}
WHERE municipality <> TRIM(municipality)