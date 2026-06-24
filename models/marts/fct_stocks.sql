-- Fait inventaire (snapshot) : grain = store_id + product_id.
-- Sert l'axe Stocks (niveau par magasin, ruptures, valorisation).

with stocks as (

    select * from {{ ref('stg_stocks') }}

),

products as (

    select * from {{ ref('stg_products') }}

),

final as (

    select
        -- clé de substitution (grain)
        {{ dbt_utils.generate_surrogate_key(['stocks.store_id', 'stocks.product_id']) }} as stock_key,

        -- clés étrangères vers les dimensions
        stocks.store_id,
        stocks.product_id,

        -- mesures
        stocks.quantity,
        round(stocks.quantity * products.list_price, 2) as stock_value,

        -- indicateur
        stocks.quantity = 0 as is_out_of_stock

    from stocks
    left join products on stocks.product_id = products.product_id

)

select * from final
