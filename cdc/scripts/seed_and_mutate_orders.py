from __future__ import annotations

import os
import time
from contextlib import closing


def _connect():
    import psycopg2  # local import for optional dependency

    return psycopg2.connect(
        host=os.getenv("CDC_PG_HOST", "localhost"),
        port=int(os.getenv("CDC_PG_PORT", "5433")),
        dbname=os.getenv("CDC_PG_DB", "coinstream"),
        user=os.getenv("CDC_PG_USER", "postgres"),
        password=os.getenv("CDC_PG_PASSWORD", "postgres"),
    )


def main() -> None:
    sleep_seconds = float(os.getenv("CDC_MUTATION_SLEEP_SECONDS", "0.8"))
    with closing(_connect()) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            # Scenario 1: insert
            cur.execute(
                """
                INSERT INTO public.orders (user_id, status, amount)
                VALUES
                    (2001, 'created', 10.00),
                    (2002, 'created', 55.50),
                    (2003, 'created', 99.99)
                RETURNING order_id
                """
            )
            order_ids = [row[0] for row in cur.fetchall()]
            conn.commit()
            print(f"[seed] inserted orders={order_ids}")

            time.sleep(sleep_seconds)

            # Scenario 2: update
            cur.execute(
                """
                UPDATE public.orders
                SET status = 'paid', amount = amount + 5.00
                WHERE order_id = %s
                """,
                (order_ids[0],),
            )
            conn.commit()
            print(f"[seed] updated order_id={order_ids[0]} to paid")

            time.sleep(sleep_seconds)

            # Scenario 3: cancel (status update)
            cur.execute(
                """
                UPDATE public.orders
                SET status = 'canceled'
                WHERE order_id = %s
                """,
                (order_ids[1],),
            )
            conn.commit()
            print(f"[seed] canceled order_id={order_ids[1]} by status update")

            time.sleep(sleep_seconds)

            # Scenario 4: delete
            cur.execute("DELETE FROM public.orders WHERE order_id = %s", (order_ids[2],))
            conn.commit()
            print(f"[seed] hard deleted order_id={order_ids[2]}")


if __name__ == "__main__":
    main()
