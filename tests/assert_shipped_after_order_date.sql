-- Test singulier (métier) : cohérence chronologique de la livraison.
-- Une commande expédiée ne peut pas l'être AVANT d'avoir été passée.
-- Renvoie les commandes en infraction (shipped_date < order_date) -> doit être vide.

select
    order_id,
    order_date,
    shipped_date
from {{ ref('fct_orders') }}
where shipped_date is not null
  and shipped_date < order_date
