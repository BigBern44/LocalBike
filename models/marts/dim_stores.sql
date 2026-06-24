-- Dimension magasin.
-- Grain : un magasin (store_id).

with stores as (

    select * from {{ ref('stg_stores') }}

),

final as (

    select
        store_id,
        store_name,
        phone,
        email,
        street,
        city,
        state,
        zip_code
    from stores

)

select * from final
