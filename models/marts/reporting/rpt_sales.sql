-- Reporting plat (dénormalisé) pour Looker Studio — axe Ventes.
-- Grain : une ligne de commande (order_id + item_id).
-- Aplatit fct_order_items + toutes ses dimensions en une table large, lisible
-- directement par le dashboard (pas de « blend » Looker Studio).
-- Sert les axes Revenu, Top produits/catégories/marques, Clients, Staff.

with order_items as (

    select * from {{ ref('fct_order_items') }}

),

products as (

    select * from {{ ref('dim_products') }}

),

stores as (

    select * from {{ ref('dim_stores') }}

),

customers as (

    select * from {{ ref('dim_customers') }}

),

staffs as (

    select * from {{ ref('dim_staffs') }}

),

dates as (

    select * from {{ ref('dim_date') }}

),

final as (

    select
        -- clé (grain)
        order_items.order_item_key,
        order_items.order_id,
        order_items.item_id,

        -- temps (depuis dim_date)
        order_items.order_date,
        dates.year       as order_year,
        dates.quarter    as order_quarter,
        dates.month      as order_month,
        dates.month_name as order_month_name,
        dates.year_month as order_year_month,
        dates.day_name   as order_day_name,
        dates.is_weekend as order_is_weekend,

        -- statut commande
        order_items.order_status,
        case order_items.order_status
            when 1 then 'Pending'
            when 2 then 'Processing'
            when 3 then 'Rejected'
            when 4 then 'Completed'
            else 'Unknown'
        end as order_status_label,

        -- produit / marque / catégorie
        order_items.product_id,
        products.product_name,
        products.brand_name,
        products.category_name,
        products.model_year,

        -- magasin
        order_items.store_id,
        stores.store_name,
        stores.city  as store_city,
        stores.state as store_state,

        -- client
        order_items.customer_id,
        customers.full_name as customer_name,
        customers.city      as customer_city,
        customers.state     as customer_state,

        -- vendeur
        order_items.staff_id,
        staffs.full_name as staff_name,

        -- mesures
        order_items.quantity,
        order_items.list_price as unit_price,
        order_items.discount,
        order_items.revenue

    from order_items
    left join products  on order_items.product_id  = products.product_id
    left join stores    on order_items.store_id    = stores.store_id
    left join customers on order_items.customer_id = customers.customer_id
    left join staffs    on order_items.staff_id    = staffs.staff_id
    left join dates     on order_items.order_date  = dates.date_day

)

select * from final
