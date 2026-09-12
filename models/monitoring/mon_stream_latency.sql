-- Supervision — latence et débit du flux temps réel, agrégés à l'heure.
-- Grain : une heure d'activité du flux (fenêtre glissante de 7 jours).
--
-- Doublon assumé avec Prometheus, et c'est le but : Prometheus mesure la latence
-- **vue par le process** (rétention courte, perdue à chaque redémarrage), cette vue
-- la reconstitue **depuis les données réellement écrites** et la conserve aussi
-- longtemps que la table d'événements. Un écart entre les deux est lui-même un
-- signal : des événements observés par le listener mais absents de BigQuery
-- signifient que le flush échoue.
--
-- `event_ts` est l'instant du COMMIT PostgreSQL, `ingested_at` celui de la réception
-- par le listener : leur écart est la latence de bout en bout du transport.

{{ config(materialized='view') }}

with events as (

    -- Fenêtre de 7 jours : élague les partitions de `event_ts` et borne le coût de
    -- chaque rafraîchissement du dashboard.
    select
        event_ts,
        ingested_at,
        event_type,
        is_below_threshold,
        timestamp_diff(ingested_at, event_ts, millisecond) as latency_ms
    from {{ source('local_bike_raw', 'stock_events_stream') }}
    where event_ts >= timestamp_sub(current_timestamp(), interval 7 day)

),

by_hour as (

    select
        timestamp_trunc(event_ts, hour)                          as event_hour,
        count(*)                                                 as events,
        countif(is_below_threshold)                              as alerts,
        countif(event_type = 'INSERT')                           as inserts,
        countif(event_type = 'UPDATE')                           as updates,
        round(avg(latency_ms) / 1000, 3)                         as avg_latency_seconds,
        round(
            approx_quantiles(latency_ms, 100)[offset(95)] / 1000, 3
        )                                                        as p95_latency_seconds,
        round(max(latency_ms) / 1000, 3)                          as max_latency_seconds,
        max(event_ts)                                            as last_event_at
    from events
    group by event_hour

)

select * from by_hour
