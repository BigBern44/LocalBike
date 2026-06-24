with source as (

    select * from {{ source('local_bike_raw', 'public_brands') }}

),

renamed as (

    select
        cast(brand_id as int64) as brand_id,
        brand_name
    from source

)

select * from renamed
