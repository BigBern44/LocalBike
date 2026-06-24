-- Fait principal : une ligne de commande (grain = order_id + item_id).
-- Porte le revenu et les clés étrangères vers toutes les dimensions du star schema.
-- Les attributs descriptifs (produit, marque...) vivent dans les dimensions, pas ici.

with order_items as (

    select * from {{ ref('int_order_items_enriched') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

final as (

    select
        -- clé de substitution (grain)
        {{ dbt_utils.generate_surrogate_key(['order_items.order_id', 'order_items.item_id']) }} as order_item_key,

        -- clés naturelles / dégénérées
        order_items.order_id,
        order_items.item_id,

        -- clés étrangères vers les dimensions
        order_items.product_id,
        orders.customer_id,
        orders.store_id,
        orders.staff_id,
        orders.order_date,            -- -> dim_date.date_day

        -- contexte commande
        orders.order_status,

        -- mesures
        order_items.quantity,
        order_items.list_price,
        order_items.discount,
        order_items.revenue

    from order_items
    left join orders on order_items.order_id = orders.order_id

)

select * from final
