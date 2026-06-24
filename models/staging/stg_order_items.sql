with source as (

    select * from {{ source('local_bike_raw', 'public_order_items') }}

),

renamed as (

    select
        cast(order_id as int64)     as order_id,
        cast(item_id as int64)      as item_id,
        cast(product_id as int64)   as product_id,
        cast(quantity as int64)     as quantity,
        cast(list_price as numeric) as list_price,
        cast(discount as numeric)   as discount
    from source

)

select * from renamed
