with source as (

    select * from {{ source('local_bike_raw', 'public_orders') }}

),

renamed as (

    select
        cast(order_id as int64)         as order_id,
        cast(customer_id as int64)      as customer_id,
        cast(order_status as int64)     as order_status,
        safe_cast(order_date as date)   as order_date,
        safe_cast(required_date as date) as required_date,
        safe_cast(shipped_date as date) as shipped_date,  -- 'NULL' littéral -> NULL via safe_cast
        cast(store_id as int64)         as store_id,
        cast(staff_id as int64)         as staff_id
    from source

)

select * from renamed
