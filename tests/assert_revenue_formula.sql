-- Test singulier (règle de calcul) : garde-fou sur la formule du revenu.
-- Vérifie que `revenue` stocké dans `fct_order_items` correspond bien à
-- quantity * list_price * (1 - discount), arrondi à 2 décimales.
-- Protège contre une régression de la logique de calcul. Doit renvoyer 0 ligne.

select
    order_id,
    item_id,
    revenue,
    round(quantity * list_price * (1 - discount), 2) as expected_revenue
from {{ ref('fct_order_items') }}
where revenue <> round(quantity * list_price * (1 - discount), 2)
