CREATE TABLE IF NOT EXISTS public.orders
(
    order_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    status VARCHAR(32) NOT NULL,
    amount NUMERIC(18, 2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.order_items
(
    order_item_id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES public.orders(order_id) ON DELETE CASCADE,
    product_code VARCHAR(64) NOT NULL,
    quantity INT NOT NULL,
    unit_price NUMERIC(18, 2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION public.set_updated_at()
RETURNS TRIGGER AS
$$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_orders_set_updated_at ON public.orders;
CREATE TRIGGER trg_orders_set_updated_at
BEFORE UPDATE ON public.orders
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at();

INSERT INTO public.orders (user_id, status, amount)
VALUES
    (1001, 'created', 120.50),
    (1002, 'created', 42.00),
    (1003, 'created', 300.25)
ON CONFLICT DO NOTHING;
