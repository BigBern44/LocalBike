-- Émetteur d'événements de stock (à exécuter UNE FOIS).
-- Où : Supabase Studio > SQL Editor (rôle postgres).
-- Consommé par : ingestion/stream_stock_alerts.py (LISTEN sur le canal `stock_events`).
--
-- Aucune clé API Supabase n'est nécessaire : le canal est natif PostgreSQL
-- (pg_notify), lu via la même connexion que l'ingestion dlt.

-- 1) Fonction trigger : publie le mouvement de stock sur le canal `stock_events`.
create or replace function public.notify_stock_change()
returns trigger
language plpgsql
as $$
begin
    -- Un UPDATE qui ne touche pas la quantité n'est pas un mouvement de stock.
    if tg_op = 'UPDATE' and old.quantity is not distinct from new.quantity then
        return new;
    end if;

    perform pg_notify(
        'stock_events',
        json_build_object(
            'event_type',      tg_op,
            'event_ts',        current_timestamp,
            'store_id',        new.store_id,
            'product_id',      new.product_id,
            'quantity_before', case when tg_op = 'UPDATE' then old.quantity end,
            'quantity_after',  new.quantity
        )::text
    );
    return new;
end;
$$;

-- 2) Déclencheur sur la table de stocks.
drop trigger if exists trg_notify_stock_change on public.stocks;

create trigger trg_notify_stock_change
after insert or update on public.stocks
for each row
execute function public.notify_stock_change();

-- 3) Vérification : le trigger doit apparaître.
select tgname, tgenabled
from pg_trigger
where tgrelid = 'public.stocks'::regclass
  and not tgisinternal;

-- 4) Démo : simule un mouvement de stock pendant que `make stream` tourne.
--    L'événement doit s'afficher dans les logs Python en moins d'une seconde.
-- update public.stocks set quantity = quantity - 3 where store_id = 1 and product_id = 1;
-- update public.stocks set quantity = 0            where store_id = 1 and product_id = 2;
