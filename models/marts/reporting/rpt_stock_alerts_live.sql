-- Reporting temps réel — axe Alertes de stock.
-- Grain : un couple magasin/produit actuellement sous son seuil d'alerte.
--
-- Matérialisé en **vue** (et non en table comme les autres `rpt_*`) : la source est
-- le flux d'événements alimenté en continu par `ingestion/stream_stock_alerts.py`.
-- Une vue est recalculée à chaque lecture de Looker Studio — donc toujours à jour,
-- sans réécriture périodique de table (BigQuery n'est pas fait pour ça : chaque
-- rafraîchissement serait une réécriture complète, et les modifications de table
-- sont plafonnées par projet et par jour).
--
-- Le seuil est lu sur l'événement lui-même (`alert_threshold`), figé à l'insertion :
-- l'historique reste cohérent même si `STOCK_ALERT_THRESHOLD` évolue.
--
-- ⚠ Ne connaît que les couples ayant bougé depuis le démarrage du flux ; l'état
-- exhaustif du stock reste porté par `rpt_stocks` (batch quotidien).

{{ config(materialized='view') }}

with events as (

    -- Fenêtre de 30 jours : élague les partitions de `event_ts` et borne le scan
    -- de chaque lecture du dashboard.
    select
        store_id,
        product_id,
        event_ts,
        quantity_before,
        quantity_after,
        alert_threshold
    from {{ source('local_bike_raw', 'stock_events_stream') }}
    where event_ts >= timestamp_sub(current_timestamp(), interval 30 day)

),

last_event as (

    -- Dernier état connu de chaque couple magasin/produit.
    select * except (rn)
    from (
        select
            events.*,
            row_number() over (
                partition by store_id, product_id order by event_ts desc
            ) as rn
        from events
    )
    where rn = 1

),

stores as (

    select * from {{ ref('dim_stores') }}

),

products as (

    select * from {{ ref('dim_products') }}

),

final as (

    select
        -- magasin
        last_event.store_id,
        stores.store_name,
        stores.city  as store_city,
        stores.state as store_state,

        -- produit / marque / catégorie
        last_event.product_id,
        products.product_name,
        products.brand_name,
        products.category_name,
        products.list_price,

        -- niveau de stock à l'instant t
        last_event.quantity_before,
        last_event.quantity_after as quantity,
        last_event.alert_threshold,
        last_event.quantity_after = 0 as is_out_of_stock,

        -- fraîcheur
        last_event.event_ts as last_movement_at,
        timestamp_diff(current_timestamp(), last_event.event_ts, minute) as minutes_since_movement

    from last_event
    left join stores   on last_event.store_id   = stores.store_id
    left join products on last_event.product_id = products.product_id
    where last_event.quantity_after <= last_event.alert_threshold

)

select * from final
