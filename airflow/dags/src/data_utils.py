from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
import logging


# =============================================================================
# Schema layout for ELT pipeline
# Purpose of each schema:
#   raw             → immutable(ish) landings from source systems; minimal shaping only.
#   staging  → cleaned/validated/intermediate transforms ready for modeling.
#   analytics → star/snowflake marts, semantic views, and BI-serving tables.
# Power BI (read-only) should query *analytics* only.
# =============================================================================

#CREATE SCHEMA IF NOT EXISTS raw;
# raw: "landing zone" for source data as-ingested.
# - Stores the closest faithful copy of upstream payloads after *light* typing.
# - No business logic or heavy joins; append-only patterns preferred.
# - Enables reproducibility and late re-processing (dbt models can always
#   be rebuilt from raw). Acts as your single source of truth snapshot.

#CREATE SCHEMA IF NOT EXISTS staging;
# staging: "refinery" zone for deterministic cleaning and standardization.
# - Apply type coercions, denormalization, de-duplication, surrogate keys,
#   basic conforming of dimensions, and integrity checks.
# - Tables here are transient/derivative; dbt models typically materialize as
#   views or ephemeral tables that feed public_analytics.

#CREATE SCHEMA IF NOT EXISTS analytics;
# analytics: "presentation" / semantic layer for consumers (BI, notebooks).
# - Star/snowflake schemas (fact_* and dim_*), or curated wide tables.
# - Column names and types are business-friendly and stable.
# - Only this schema will be granted to the bi_read role for least-privilege.


DB_NAME = os.environ.get("ELT_DATABASE_NAME")

def create_elt_schemas():
    try:
        # Create hook for the already existing connection in airflow
        hook = PostgresHook(postgres_conn_id="postgres_default", database=DB_NAME)

    # Connect and execute SQL
    
        with hook.get_conn() as conn:
            with conn.cursor() as cur:

                for schema in ["raw", "staging", "analytics"]:
                    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
                    logging.info(f"Schema '{schema}' ensured.")


                cur.execute("""
                    CREATE TABLE IF NOT EXISTS raw.customers (
                        customer_id UUID PRIMARY KEY,
                        company_name TEXT,
                        country TEXT,
                        industry TEXT,
                        company_size TEXT,
                        signup_date TIMESTAMPTZ,
                        updated_at TIMESTAMPTZ,
                        is_churned BOOLEAN
                    );
                """)
                logging.info("Table 'raw.customers' ensured.")


                cur.execute("""
                    CREATE TABLE IF NOT EXISTS raw.payments (
                        payment_id UUID PRIMARY KEY,
                        customer_id UUID,
                        product TEXT,
                        amount NUMERIC,
                        currency TEXT,
                        status TEXT,
                        refunded_amount NUMERIC,
                        fee NUMERIC,
                        payment_method TEXT,
                        country TEXT,
                        created_at TIMESTAMPTZ,
                        updated_at TIMESTAMPTZ
                    );
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_payments_created ON raw.payments(created_at);")
                logging.info("Table 'raw.payments' and index ensured.")


                cur.execute("""
                    CREATE TABLE IF NOT EXISTS raw.sessions (
                        session_id UUID PRIMARY KEY,
                        customer_id UUID,
                        source TEXT,
                        medium TEXT,
                        campaign TEXT,
                        device TEXT,
                        country TEXT,
                        pageviews INT,
                        session_duration_s INT,
                        bounced INT,
                        converted INT,
                        session_start TIMESTAMPTZ,
                        updated_at TIMESTAMPTZ
                    );
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_sessions_start ON raw.sessions(session_start);")
                logging.info("Table 'raw.sessions' and index ensured.")
                
            conn.commit()
            logging.info("All schemas and raw tables created successfully.")


    except Exception as e:
        logging.error("Failed to create ELT schemas/tables", exc_info=True)
        if 'conn' in locals():
            conn.rollback()
        raise  # Makes the DAG fail - if ommitted dag would keep running.
