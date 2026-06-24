with source as (

    select * from {{ source('local_bike_raw', 'public_customers') }}

),

renamed as (

    select
        cast(customer_id as int64) as customer_id,
        first_name,
        last_name,
        nullif(phone, 'NULL')      as phone,
        nullif(email, 'NULL')      as email,
        street,
        city,
        state,
        cast(zip_code as int64)    as zip_code
    from source

)

select * from renamed
