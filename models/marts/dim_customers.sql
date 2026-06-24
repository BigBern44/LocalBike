-- Dimension client.
-- Grain : un client (customer_id).

with customers as (

    select * from {{ ref('stg_customers') }}

),

final as (

    select
        customer_id,
        first_name,
        last_name,
        concat(first_name, ' ', last_name) as full_name,
        email,
        phone,
        street,
        city,
        state,
        zip_code
    from customers

)

select * from final
