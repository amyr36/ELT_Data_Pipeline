
import os
import sys
import time
import logging
from contextlib import closing

import psycopg2
from psycopg2 import OperationalError, sql
from psycopg2.extras import execute_values


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("elt")


# Database connection settings
SOURCE = {
    "host": os.getenv("SOURCE_HOST", "source_db"),
    "port": int(os.getenv("SOURCE_PORT", 5432)),
    "dbname": os.getenv("SOURCE_DB", "postgres"),
    "user": os.getenv("SOURCE_USER", "postgres"),
    "password": os.getenv("SOURCE_PASSWORD", ""),
}

DEST = {
    "host": os.getenv("DEST_HOST", "destination_db"),
    "port": int(os.getenv("DEST_PORT", 5432)),
    "dbname": os.getenv("DEST_DB", "postgres"),
    "user": os.getenv("DEST_USER", "postgres"),
    "password": os.getenv("DEST_PASSWORD", ""),
}

TABLES = [
    "users",
    "films",
    "actors",
    "film_category",
    "film_actors",
]

BATCH_SIZE = 5000
INSERT_PAGE_SIZE = 1000


def wait_for_db(
    config: dict,
    name: str,
    retries: int = 30,
    delay: int = 2,
):
    # Wait until the database accepts connections
    for attempt in range(1, retries + 1):
        try:
            with closing(
                psycopg2.connect(
                    **config,
                    connect_timeout=3,
                )
            ):
                pass

            log.info("[%s] healthy", name)
            return

        except OperationalError as exc:
            log.warning(
                "[%s] not ready (%s/%s): %s",
                name, attempt, retries, exc,
            )
            time.sleep(delay)

    raise RuntimeError(
        f"[{name}] did not become healthy"
    )


def count_rows(conn, table: str) -> int:
    # Return the number of rows in a table
    query = sql.SQL(
        "SELECT COUNT(*) FROM {}"
    ).format(sql.Identifier(table))

    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchone()[0]


def transfer_table(
    src_conn,
    dest_conn,
    table: str,
) -> int:
    # Transfer rows in batches using a server-side cursor
    select_query = sql.SQL(
        "SELECT * FROM {}"
    ).format(sql.Identifier(table))

    total_loaded = 0

    with src_conn.cursor(
        name=f"transfer_{table}"
    ) as src_cur:
        src_cur.itersize = BATCH_SIZE
        src_cur.execute(select_query)

        # Read source column names
        columns = [
            desc[0] for desc in src_cur.description
        ]

        # Build a safe INSERT statement
        insert_query = sql.SQL(
            "INSERT INTO {} ({}) VALUES %s"
        ).format(
            sql.Identifier(table),
            sql.SQL(", ").join(
                sql.Identifier(column)
                for column in columns
            ),
        ).as_string(dest_conn)

        while True:
            # Fetch the next batch
            rows = src_cur.fetchmany(BATCH_SIZE)

            if not rows:
                break

            # Insert the current batch
            with dest_conn.cursor() as dest_cur:
                execute_values(
                    dest_cur,
                    insert_query,
                    rows,
                    page_size=INSERT_PAGE_SIZE,
                )

            total_loaded += len(rows)

            log.info(
                "[transfer] %s: loaded %s rows",
                table,
                total_loaded,
            )

    return total_loaded


def main() -> int:
    log.info("=== ELT pipeline starting ===")

    try:
        # Wait for both databases
        wait_for_db(SOURCE, "source")
        wait_for_db(DEST, "destination")

        with closing(
            psycopg2.connect(**SOURCE)
        ) as src_conn:

            # Keep a consistent source snapshot
            src_conn.set_session(
                isolation_level="REPEATABLE READ",
                readonly=True,
            )

            with closing(
                psycopg2.connect(**DEST)
            ) as dest_conn:

                # Commit all changes only on success
                with dest_conn:

                    # Remove old destination data
                    truncate_query = sql.SQL(
                        "TRUNCATE TABLE {}"
                    ).format(
                        sql.SQL(", ").join(
                            sql.Identifier(table)
                            for table in TABLES
                        )
                    )

                    with dest_conn.cursor() as cur:
                        cur.execute(truncate_query)

                    log.info("[dest] old data truncated")

                    # Transfer every table
                    for table in TABLES:
                        loaded = transfer_table(
                            src_conn,
                            dest_conn,
                            table,
                        )

                        log.info(
                            "[transfer] %s: loaded %s rows",
                            table,
                            loaded,
                        )

                    # Verify row counts before commit
                    for table in TABLES:
                        src_count = count_rows(
                            src_conn, table
                        )
                        dest_count = count_rows(
                            dest_conn, table
                        )

                        if src_count != dest_count:
                            raise RuntimeError(
                                f"{table}: count mismatch; "
                                f"source={src_count}, "
                                f"destination={dest_count}"
                            )

                        log.info(
                            "[verify] %s: source=%s dest=%s OK",
                            table,
                            src_count,
                            dest_count,
                        )

                # The destination transaction has committed
                log.info(
                    "=== ELT pipeline committed successfully ==="
                )

        return 0

    except Exception:
        # Roll back uncommitted destination changes
        log.exception("ELT pipeline failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
