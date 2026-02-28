WITH ranked AS (
    SELECT
        product_id,
        product_name,
        brand_name,
        category,
        product_url,
        snapshot_date,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY snapshot_date DESC
        ) AS rn
    FROM {{ ref('stg_product_snapshots') }}
)

SELECT
    product_id,
    product_name,
    brand_name,
    category,
    product_url
FROM ranked
WHERE rn = 1
