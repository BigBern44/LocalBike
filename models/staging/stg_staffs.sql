with source as (

    select * from {{ source('local_bike_raw', 'public_staffs') }}

),

renamed as (

    select
        cast(staff_id as int64)                        as staff_id,
        first_name,
        last_name,
        nullif(email, 'NULL')                          as email,
        nullif(phone, 'NULL')                          as phone,
        cast(active as int64) = 1                      as active,
        cast(store_id as int64)                        as store_id,
        safe_cast(nullif(manager_id, 'NULL') as int64) as manager_id
    from source

)

select * from renamed
