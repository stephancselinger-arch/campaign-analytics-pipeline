-- ClickHouse schema for the ad-event analytics pipeline.
-- Loaded automatically by the clickhouse container on first start
-- (mounted into /docker-entrypoint-initdb.d).

CREATE DATABASE IF NOT EXISTS analytics;

-- Raw event table. MergeTree partitioned by day, ordered for the common
-- "by campaign over a time range" access pattern.
CREATE TABLE IF NOT EXISTS analytics.ad_events
(
    event_id            String,
    event_type          LowCardinality(String),
    event_time          DateTime,
    event_date          Date,
    campaign_id         String,
    line_item_id        String,
    creative_id         String,
    advertiser_id       String,
    publisher_domain    String,
    placement_type      LowCardinality(String),
    device_type         LowCardinality(String),
    geo_country         LowCardinality(String),
    user_id             String,
    bid_price_cpm       Float64,
    clearing_price_cpm  Float64,
    revenue_usd         Float64,
    conversion_value_usd Float64,
    is_impression       UInt8,
    is_click            UInt8,
    is_win              UInt8,
    is_conversion       UInt8
)
ENGINE = MergeTree
PARTITION BY event_date
ORDER BY (campaign_id, event_date, event_type);

-- Pre-aggregated daily rollup per campaign, maintained incrementally by a
-- materialized view. Reading campaign KPIs becomes a scan of a tiny table.
CREATE TABLE IF NOT EXISTS analytics.campaign_daily
(
    event_date    Date,
    campaign_id   String,
    impressions   UInt64,
    clicks        UInt64,
    wins          UInt64,
    conversions   UInt64,
    spend_usd     Float64
)
ENGINE = SummingMergeTree
PARTITION BY event_date
ORDER BY (campaign_id, event_date);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.campaign_daily_mv
TO analytics.campaign_daily AS
SELECT
    event_date,
    campaign_id,
    sum(is_impression) AS impressions,
    sum(is_click)      AS clicks,
    sum(is_win)        AS wins,
    sum(is_conversion) AS conversions,
    sum(revenue_usd)   AS spend_usd
FROM analytics.ad_events
GROUP BY event_date, campaign_id;

-- Example KPI query (CTR / CVR / CPC / CPA) from the rollup table:
--
--   SELECT campaign_id,
--          sum(impressions) AS impressions,
--          sum(clicks)      AS clicks,
--          sum(spend_usd)   AS spend,
--          round(sum(clicks)      / nullIf(sum(impressions), 0), 4) AS ctr,
--          round(sum(conversions) / nullIf(sum(clicks), 0), 4)      AS cvr,
--          round(sum(spend_usd)   / nullIf(sum(clicks), 0), 4)      AS cpc,
--          round(sum(spend_usd)   / nullIf(sum(conversions), 0), 4) AS cpa
--   FROM analytics.campaign_daily
--   WHERE event_date BETWEEN %(from)s AND %(to)s
--   GROUP BY campaign_id;
