CREATE TEMP MACRO day_ms() AS 86400000;
CREATE TEMP MACRO lookback_ms() AS (7 * day_ms());
CREATE TEMP MACRO utc_seconds(value) AS epoch_ms(try_strptime(value, '%Y-%m-%dT%H:%M:%S'));
CREATE TEMP MACRO parse_unsigned(value) AS (
    CASE WHEN regexp_full_match(value, '[0-9]+')
         THEN try_cast(coalesce(nullif(ltrim(value, '0'), ''), '0') AS BIGINT)
    END
);
CREATE TEMP MACRO parse_ts(value) AS (
    CASE
        WHEN regexp_full_match(value, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z')
            THEN utc_seconds(left(value, 19))
        WHEN regexp_full_match(value,
                '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-]([01][0-9]|2[0-3]):[0-5][0-9]')
            THEN utc_seconds(left(value, 19))
                - CASE WHEN substr(value, 20, 1) = '+' THEN 1 ELSE -1 END
                  * (cast(substr(value, 21, 2) AS INTEGER) * 60 + cast(right(value, 2) AS INTEGER)) * 60000
        ELSE parse_unsigned(value)
    END
);

CREATE TEMP TABLE events AS
WITH deduplicated AS (
    SELECT unnest(arg_min(struct_pack(user_id, ts, ingest_ts, event_type, product_id, price_cents), ingest_ts))
    FROM event_rows
    WHERE event_id IS NOT NULL
    GROUP BY event_id
), parsed AS (
    SELECT user_id, parse_ts(ts) AS event_ms, utc_seconds(left(ingest_ts, 19)) AS ingest_ms, product_id,
           CASE WHEN event_type = 'purchase' THEN parse_unsigned(price_cents) ELSE 0 END AS revenue
    FROM deduplicated
    WHERE user_id IS NOT NULL AND product_id IS NOT NULL
        AND event_type IN ('view', 'cart', 'purchase')
        AND (event_type <> 'purchase' OR parse_unsigned(price_cents) BETWEEN 1 AND 10000000)
)
SELECT coalesce(a.person, e.user_id) AS person, event_ms, revenue,
       CASE WHEN revenue > 0 THEN coalesce(p.category, 'unknown') END AS category
FROM parsed e
CROSS JOIN run_window w
LEFT JOIN aliases a USING (user_id)
LEFT JOIN products p ON e.revenue > 0 AND e.product_id = p.product_id
WHERE event_ms >= w.start_ms AND event_ms < w.end_ms AND ingest_ms - event_ms <= day_ms();

DROP TABLE event_rows;
DROP TABLE products;

CREATE TEMP TABLE session_events AS
WITH gaps AS (
    SELECT *, lag(event_ms) OVER (PARTITION BY person ORDER BY event_ms) AS previous_ms
    FROM events
)
SELECT person, revenue, category,
       max(CASE WHEN previous_ms IS NULL OR event_ms - previous_ms > 1800000 THEN event_ms END)
           OVER (PARTITION BY person ORDER BY event_ms RANGE UNBOUNDED PRECEDING) AS start_ms
FROM gaps;

DROP TABLE events;

CREATE TEMP TABLE sessions AS
SELECT person, start_ms, cast(make_timestamp_ms(start_ms) AS DATE) AS day,
       count(*) AS events, sum(revenue)::BIGINT AS revenue_cents
FROM session_events
GROUP BY person, start_ms;

CREATE TEMP TABLE user_daily AS
WITH totals AS (
    SELECT person AS user_id, day, count(*) AS sessions, sum(events)::BIGINT AS events,
           sum(revenue_cents)::BIGINT AS revenue_cents
    FROM sessions
    GROUP BY person, day
), category_totals AS (
    SELECT person AS user_id, cast(make_timestamp_ms(start_ms) AS DATE) AS day,
           category, sum(revenue) AS revenue
    FROM session_events
    WHERE category IS NOT NULL
    GROUP BY user_id, day, category
), top_categories AS (
    SELECT user_id, day, first(category ORDER BY revenue DESC, category) AS top_category
    FROM category_totals
    GROUP BY user_id, day
)
SELECT totals.*, top_category
FROM totals LEFT JOIN top_categories USING (user_id, day);

DROP TABLE session_events;

CREATE TEMP TABLE clicks AS
WITH parsed AS (
    SELECT coalesce(a.person, c.user_id) AS person, c.campaign_id, click_id, parse_ts(ts) AS click_ms
    FROM click_rows c
    JOIN campaigns USING (campaign_id)
    LEFT JOIN aliases a USING (user_id)
    WHERE click_id IS NOT NULL AND c.user_id IS NOT NULL
)
SELECT person, campaign_id, click_ms, min(click_id) AS click_id
FROM parsed CROSS JOIN run_window
WHERE click_ms >= start_ms - lookback_ms() AND click_ms < end_ms
GROUP BY person, campaign_id, click_ms;

DROP TABLE click_rows;
DROP TABLE aliases;

CREATE TEMP TABLE campaign_daily AS
WITH latest_click_candidates AS (
    -- Break ties before joining each session with the most recent click.
    SELECT person, click_ms, arg_min(campaign_id, click_id) AS campaign_id
    FROM clicks
    GROUP BY person, click_ms
), attributed AS (
    SELECT s.day, s.revenue_cents,
           CASE WHEN s.start_ms - c.click_ms <= lookback_ms() THEN c.campaign_id ELSE 'organic' END AS campaign_id
    FROM sessions s
    ASOF LEFT JOIN latest_click_candidates c ON s.person = c.person AND s.start_ms >= c.click_ms
), credited AS (
    SELECT campaign_id, day, count(*) AS sessions,
           count(*) FILTER (WHERE revenue_cents > 0) AS purchasing_sessions,
           sum(revenue_cents)::BIGINT AS revenue_cents
    FROM attributed
    GROUP BY campaign_id, day
), click_gaps AS (
    SELECT *, lag(click_ms) OVER (PARTITION BY person, campaign_id ORDER BY click_ms) AS previous_ms
    FROM clicks
), click_intervals AS (
    SELECT *, max(CASE WHEN previous_ms IS NULL OR click_ms - previous_ms > lookback_ms() + 1 THEN click_ms END)
        OVER (PARTITION BY person, campaign_id ORDER BY click_ms ROWS UNBOUNDED PRECEDING) AS first_click_ms
    FROM click_gaps
), coverage AS (
    -- Merge overlapping lookback windows so a campaign assists each session once.
    SELECT person, campaign_id, first_click_ms, max(click_ms) + lookback_ms() AS last_covered_ms
    FROM click_intervals
    GROUP BY person, campaign_id, first_click_ms
), assists AS (
    SELECT c.campaign_id, s.day, count(*) AS assisted_sessions
    FROM sessions s
    JOIN coverage c ON s.person = c.person AND s.start_ms BETWEEN c.first_click_ms AND c.last_covered_ms
    GROUP BY c.campaign_id, s.day
)
SELECT campaign_id, coalesce(c.channel, 'organic') AS channel, day,
       coalesce(sessions, 0) AS sessions, coalesce(purchasing_sessions, 0) AS purchasing_sessions,
       coalesce(revenue_cents, 0) AS revenue_cents, coalesce(assisted_sessions, 0) AS assisted_sessions
FROM credited FULL JOIN assists USING (campaign_id, day)
LEFT JOIN campaigns c USING (campaign_id);
