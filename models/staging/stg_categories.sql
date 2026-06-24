with source as (

    select * from {{ source('local_bike_raw', 'public_categories') }}

),

renamed as (

    select
        cast(category_id as int64) as category_id,
        category_name
    from source

)

select * from renamed
