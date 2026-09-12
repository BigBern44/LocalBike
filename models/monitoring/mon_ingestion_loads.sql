-- Supervision — historique des chargements dlt.
-- Grain : un `load_id` dlt (une passe d'ingestion Supabase -> BigQuery).
--
-- Répond à l'indicateur « le batch tourne-t-il, et à l'heure ? ». Dagster connaît
-- ses propres runs, mais son historique vit dans son instance locale : cette vue
-- porte la même information dans l'entrepôt, donc consultable par Grafana sans
-- dépendre de l'orchestrateur — y compris pour diagnostiquer pourquoi il s'est tu.
--
-- `_dlt_loads` est la table de traçabilité écrite par dlt à chaque passe
-- (`status = 0` = chargement finalisé). Lue en direct, comme `__TABLES__` :
-- métadonnée technique, pas table métier.

{{ config(materialized='view') }}

with loads as (

    select
        load_id,
        schema_name,
        status,
        inserted_at
    from `{{ target.project }}.{{ var('raw_dataset') }}._dlt_loads`

),

sequenced as (

    select
        *,
        -- Écart avec la passe précédente : un trou bien plus large que 24 h révèle
        -- un run manqué, là où un simple « dernier chargement OK » resterait muet.
        lag(inserted_at) over (order by inserted_at) as previous_load_at
    from loads

),

final as (

    select
        load_id,
        schema_name,
        status,
        status = 0                                                       as is_success,
        inserted_at                                                      as loaded_at,
        previous_load_at,
        timestamp_diff(inserted_at, previous_load_at, minute)             as minutes_since_previous_load,
        timestamp_diff(current_timestamp(), inserted_at, minute)          as minutes_ago,
        row_number() over (order by inserted_at desc) = 1                 as is_latest
    from sequenced

)

select * from final
