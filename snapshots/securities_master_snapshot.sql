{% snapshot securities_master_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='security_id',
        strategy='timestamp',
        updated_at='updated_at',
        tags=['snapshots', 'securities']
    )
}}

-- =========================================================
-- SNAPSHOT: Securities Master Historical Tracking
-- =========================================================
-- PURPOSE:
-- Track all changes to securities master data over time
-- using SCD Type 2 (Slowly Changing Dimension)
--
-- USE CASES:
-- - Audit trail: "What was this security called 6 months ago?"
-- - Point-in-time reporting: "Show positions as of Dec 31, 2023"
-- - Regulatory compliance: Prove historical data accuracy
-- - Change tracking: When did ticker symbol change?
--
-- HOW IT WORKS:
-- - Compares 'updated_at' timestamp to detect changes
-- - When a change is detected, closes old record and creates new one
-- - Adds dbt_valid_from and dbt_valid_to columns
-- - Current records have dbt_valid_to = NULL
-- =========================================================

select
    security_id,
    isin,
    cusip,
    ticker,
    security_name,
    asset_class,
    currency,
    country,
    sector,
    market_cap_category,
    exchange,
    current_timestamp() as updated_at

from {{ source('raw_data', 'securities_master') }}

{% endsnapshot %}
