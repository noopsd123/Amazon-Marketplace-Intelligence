WITH source AS (
    SELECT * FROM {{ source('raw', 'product_snapshots') }}
),

cleaned AS (
    SELECT
        -- Identifiers
        COALESCE(NULLIF(TRIM(sku), ''), 'unknown')          AS product_id,
        _source_file                                         AS source_file,
        CAST(_loaded_at AS TIMESTAMP)                        AS loaded_at,

        -- Product attributes
        TRIM(name)                                           AS product_name,
        TRIM(brandName)                                      AS brand_name,
        TRIM(nodeName)                                       AS category,
        TRIM(url)                                            AS product_url,

        -- Pricing (strip $ and commas, cast to number)
        SAFE_CAST(
            REGEXP_REPLACE(COALESCE(salePrice, '0'), r'[^0-9.]', '')
            AS FLOAT64
        )                                                    AS sale_price,

        SAFE_CAST(
            REGEXP_REPLACE(COALESCE(listedPrice, '0'), r'[^0-9.]', '')
            AS FLOAT64
        )                                                    AS listed_price,

        TRIM(currency)                                       AS currency,

        -- Metrics
        SAFE_CAST(rating AS FLOAT64)                         AS rating,
        SAFE_CAST(
            REGEXP_REPLACE(COALESCE(CAST(reviewCount AS STRING), '0'), r'[^0-9]', '')
            AS INT64
        )                                                    AS review_count,

        -- Availability
        CASE
            WHEN LOWER(CAST(inStock AS STRING)) = 'true'  THEN TRUE
            WHEN LOWER(CAST(inStock AS STRING)) = 'false' THEN FALSE
            ELSE NULL
        END                                                  AS in_stock,

        -- Snapshot date (parse from scrapedDate field)
        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', scrapedDate) AS scraped_at,
        DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', scrapedDate)) AS snapshot_date

    FROM source
    WHERE name IS NOT NULL
      AND sku  IS NOT NULL
)

SELECT * FROM cleaned
WHERE sale_price > 0
  OR listed_price > 0
