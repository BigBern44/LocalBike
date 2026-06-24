with source as (

    select * from {{ source('local_bike_raw', 'public_stores') }}

),

renamed as (

    select
        cast(store_id as int64) as store_id,
        store_name,
        nullif(phone, 'NULL')   as phone,
        nullif(email, 'NULL')   as email,
        street,
        city,
        state,
        cast(zip_code as int64) as zip_code
    from source

)

select * from renamed
