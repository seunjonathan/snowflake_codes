WITH
 -- step 2: calculate the next updated_at value
get_updated_date_cte AS (
 SELECT *,
         LEAD(UPDATED_AT) OVER (PARTITION BY SERVICE_PROVIDER_ID ORDER BY UPDATED_AT) AS next_updated_at
  FROM
(
        SELECT
            p.SERVICE_PROVIDER_ID,
            p.PHARMACY_NAME,
            p.CREATED_AT,
            p.UPDATED_AT
        FROM
            PHARMACY_DISTINCT_CTE as p
)

-- step 3: Implement the SCD2 and generate the SK logic
)
SELECT hash(*) as sk_pharmacy, * exclude next_updated_at,
       IFF(next_updated_at IS NULL, NULL, DATEADD(DAY, -1, next_updated_at)) AS EFFECTIVE_END_DATE,
       ROW_NUMBER() OVER (
           PARTITION BY SERVICE_PROVIDER_ID
           ORDER BY UPDATED_AT
       ) AS __rec_version,
       IFF(next_updated_at IS NULL, TRUE, FALSE) AS ACTIVE
FROM get_updated_date_cte;
