WITH base AS (
    SELECT * FROM {{ ref('stg_product_snapshots') }}
),

with_changes AS (
    SELECT
        product_id,
        product_name,
        brand_name,
        category,
        snapshot_date,
        sale_price,
        listed_price,
        currency,
        rating,
        review_count,
        in_stock,
        source_file,

        -- Discount amount and percentage
        ROUND(listed_price - sale_price, 2)                  AS discount_amount,
        ROUND(
            SAFE_DIVIDE(listed_price - sale_price, listed_price) * 100
        , 2)                                                  AS discount_pct,

        -- Week-over-week price change
        ROUND(sale_price - LAG(sale_price) OVER (
            PARTITION BY product_id ORDER BY snapshot_date
        ), 2)                                                 AS price_change_wow,

        ROUND(SAFE_DIVIDE(
            sale_price - LAG(sale_price) OVER (
                PARTITION BY product_id ORDER BY snapshot_date
            ),
            LAG(sale_price) OVER (
                PARTITION BY product_id ORDER BY snapshot_date
            )
        ) * 100, 2)                                          AS price_change_pct_wow,

        -- Week-over-week rating change
        ROUND(rating - LAG(rating) OVER (
            PARTITION BY product_id ORDER BY snapshot_date
        ), 2)                                                 AS rating_change_wow,

        -- New reviews this week
        review_count - LAG(review_count) OVER (
            PARTITION BY product_id ORDER BY snapshot_date
        )                                                     AS new_reviews_wow,

        -- 4-week price range (volatility)
        ROUND(
            MAX(sale_price) OVER (
                PARTITION BY product_id
                ORDER BY snapshot_date
                ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
            ) -
            MIN(sale_price) OVER (
                PARTITION BY product_id
                ORDER BY snapshot_date
                ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
            )
        , 2)                                                  AS price_range_4wk

    FROM base
)

SELECT DISTINCT
    TO_HEX(MD5(CONCAT(product_id, CAST(snapshot_date AS STRING)))) AS snapshot_id,
    product_id,
    product_name,
    brand_name,
    category,
    snapshot_date,
    sale_price,
    listed_price,
    currency,
    rating,
    review_count,
    in_stock,
    source_file,
    discount_amount,
    discount_pct,
    price_change_wow,
    price_change_pct_wow,
    rating_change_wow,
    new_reviews_wow,
    price_range_4wk
FROM with_changes
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY product_id, snapshot_date
    ORDER BY sale_price DESC
) = 1
