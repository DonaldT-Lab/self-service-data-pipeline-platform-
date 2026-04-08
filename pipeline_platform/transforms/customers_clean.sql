CREATE OR REPLACE TABLE {{ target_table }} AS
SELECT
    customer_id,
    name,
    lower(email) AS email
FROM {{ source_table }};