-- Supervision — fraîcheur et volumétrie des tables brutes.
-- Grain : une table source (`local_bike_raw.public_*`), soit 9 lignes.
--
-- Répond à l'indicateur « les données sont-elles à jour ? » : c'est la contrepartie
-- BigQuery de ce que Prometheus mesure côté infrastructure. Le batch dlt tournant à
-- 05:00, une table non rafraîchie depuis plus de `raw_freshness_sla_hours` signale un
-- run manqué ou en échec — même si Dagster, lui, n'a rien signalé (cas typique : la
-- machine était éteinte à l'heure du schedule).
--
-- Matérialisé en **vue** : Grafana interroge la fraîcheur à l'instant de l'affichage.
-- Une table figerait la mesure à la dernière exécution de dbt — c'est-à-dire
-- précisément au moment où le pipeline allait bien.
--
-- La source est `__TABLES__`, méta-table de dataset : lue en direct plutôt que via
-- `source()`, car c'est une métadonnée BigQuery et non une table métier (pas de
-- lineage à en tirer). `stock_events_stream` en est exclue : alimentée en continu,
-- elle relève de `mon_stream_latency` et de Prometheus, pas d'un SLA batch.

{{ config(materialized='view') }}

with raw_tables as (

    select
        table_id,
        row_count,
        size_bytes,
        timestamp_millis(last_modified_time) as last_loaded_at
    from `{{ target.project }}.{{ var('raw_dataset') }}.__TABLES__`
    where starts_with(table_id, 'public_')

),

final as (

    select
        table_id                                                as table_name,
        -- Nom de la table Supabase d'origine (`public_orders` -> `orders`).
        substr(table_id, length('public_') + 1)                  as source_table,
        row_count,
        size_bytes,
        round(size_bytes / 1048576, 2)                           as size_mb,
        last_loaded_at,
        round(
            timestamp_diff(current_timestamp(), last_loaded_at, minute) / 60.0, 2
        )                                                        as hours_since_last_load,
        {{ var('raw_freshness_sla_hours') }}                     as sla_hours,
        timestamp_diff(current_timestamp(), last_loaded_at, hour)
            > {{ var('raw_freshness_sla_hours') }}               as is_stale,
        -- Une table vide après un chargement est une anomalie plus grave qu'un retard :
        -- le pipeline a « réussi » en ne produisant rien.
        row_count = 0                                            as is_empty
    from raw_tables

)

select * from final
