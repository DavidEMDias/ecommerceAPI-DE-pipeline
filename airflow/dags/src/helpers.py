import os, time, requests, logging
from datetime import datetime, timedelta, timezone
from dateutil import parser
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

API_BASE = os.environ.get("API_BASE_URL", "http://mock-api:8000")
API_KEY = os.environ["API_KEY"]
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

def get_watermark(name, default_iso): # name of the variable in airflow (ex: last_sync_time)
    return Variable.get(name, default_var=default_iso) # Example last_sync = get_watermark("last_sync_time", "2025-01-01T00:00:00Z"); saved value. if its the first execution uses the default

def set_watermark(name, iso): # save or update airflow variable
    Variable.set(name, iso)

def fetch_paged(endpoint, params, max_retries=5): #Endpoint (Customers, Sessions), Params(filters, dates), Max retries number if request fails
    """Iterate over pages of API results, returning data gradually."""

    page = 1 # Start in first page
    while True:
        p = params.copy() # Create a copy of original parameters to not overwrite external parameters 
        p.update({"page": page, "page_size": 500}) # Add Pagination Parameters (updates p dictionary)
        r = None

        for attempt in range(max_retries): # Tries to do the request until max retries 
            try:
                r = requests.get(f"{API_BASE}/{endpoint}",
                    headers=HEADERS,
                    params=p,
                    timeout=30,
                )
            except requests.exceptions.RequestException as e: # If any error occurs log the warning, wait 2**attemp number seconds and continue trying
                logging.warning("Request to %s failed (%s)", endpoint, e)
                time.sleep(2 ** attempt)
                continue

            #if r.status_code == 429:
                #retry = r.json().get("retry_after", 10)
                #time.sleep(int(retry))
                #continue
            #if r.status_code >= 500:
                #time.sleep(2 ** attempt)
                #continue

            r.raise_for_status() # HTTP >= 400 raises an exception
            data = r.json() # Convert JSON into Dictionary
            yield data # Returns each page of data - without the need to load everything into memory
            break

        else:
            logging.error(
                "Failed to fetch %s with params %s after %s attempts (last status %s)",
                endpoint,
                p,
                max_retries,
                getattr(r, "status_code", "unknown"),
            )
            raise RuntimeError(
                f"Failed to fetch {endpoint} after {max_retries} attempts",
            )
        


        if data.get("next_page"): # If there is a next page, update page and while loop continues. Null in JSON turns into None = False
            page = data["next_page"]
        else:
            return
        

def extract_and_ingest_table(table, endpoint, ts_field, wm_name):
    """
    Extract data from API and load into raw table.

    Args:
        table (str): Target raw table (e.g., "raw.customers").
        endpoint (str): API endpoint.
        ts_field (str): Field to use for watermark (updated_at).
        wm_name (str): Watermark name in metadata - e.g. wm_customers, wm_sessions.
    Returns:
        int: Number of rows processed.
    """
    pg = PostgresHook(postgres_conn_id="postgres_default")
    iso_default = (datetime.utcnow() - timedelta(days=365)).isoformat() + "Z"

    watermark = get_watermark(wm_name, iso_default)
    max_seen = parser.isoparse(watermark)
    updated_since_param = max_seen.isoformat().replace("+00:00", "Z")


    rows = 0

   

    with pg.get_conn() as conn: # Open connection only one time
        with conn.cursor() as cur:

            for page in fetch_paged(endpoint, {"updated_since": updated_since_param}): # final dictionary to be sent as query string to request get {"updated_since": watermark,"page": 1,"page_size": 500}

                data = page.get("data") or []
                if not data: # True if [], None, "" or 0
                    continue
                
                for d in data:
                    d["last_seen_at"] = max(
                        parser.isoparse(d["updated_at"]),
                        parser.isoparse(d.get("last_seen_at") or datetime.now(timezone.utc).isoformat())
                    ) # update last_seen_at for each record in the response/page, adding it to the data (dictionary)

                
                try:    
                    if table == "raw.customers":
                        cur.executemany(
                            """
                            INSERT INTO raw.customers(customer_id, company_name, country, industry, company_size, signup_date, updated_at, is_churned, last_seen_at, is_deleted)
                            VALUES(%(customer_id)s, %(company_name)s, %(country)s, %(industry)s, %(company_size)s, %(signup_date)s, %(updated_at)s, %(is_churned)s,  %(last_seen_at)s, FALSE)
                            ON CONFLICT (customer_id) DO UPDATE SET
                                company_name=EXCLUDED.company_name,
                                country=EXCLUDED.country,
                                industry=EXCLUDED.industry,
                                company_size=EXCLUDED.company_size,
                                signup_date=EXCLUDED.signup_date,
                                updated_at=EXCLUDED.updated_at,
                                is_churned=EXCLUDED.is_churned,
                                last_seen_at = EXCLUDED.last_seen_at,
                                is_deleted = FALSE
                            """,
                            data
                        )
                    elif table == "raw.payments":
                        cur.executemany(
                            """
                            INSERT INTO raw.payments(payment_id, customer_id, product, amount, currency, status, refunded_amount, fee, payment_method, country, created_at, updated_at)
                            VALUES(%(payment_id)s, %(customer_id)s, %(product)s, %(amount)s, %(currency)s, %(status)s, %(refunded_amount)s, %(fee)s, %(payment_method)s, %(country)s, %(created_at)s, %(updated_at)s)
                            ON CONFLICT (payment_id) DO UPDATE SET
                                customer_id=EXCLUDED.customer_id,
                                product=EXCLUDED.product,
                                amount=EXCLUDED.amount,
                                currency=EXCLUDED.currency,
                                status=EXCLUDED.status,
                                refunded_amount=EXCLUDED.refunded_amount,
                                fee=EXCLUDED.fee,
                                payment_method=EXCLUDED.payment_method,
                                country=EXCLUDED.country,
                                created_at=EXCLUDED.created_at,
                                updated_at=EXCLUDED.updated_at
                            """,
                            data
                        )
                    else:  # sessions
                        for d in data:
                            # API returns booleans; cast to ints for raw schema
                            d["bounced"] = int(d.get("bounced", False))
                            d["converted"] = int(d.get("converted", False))
                        cur.executemany(
                            """
                            INSERT INTO raw.sessions(session_id, customer_id, source, medium, campaign, device, country, pageviews, session_duration_s, bounced, converted, session_start, updated_at)
                            VALUES(%(session_id)s, %(customer_id)s, %(source)s, %(medium)s, %(campaign)s, %(device)s, %(country)s, %(pageviews)s, %(session_duration_s)s, %(bounced)s, %(converted)s, %(session_start)s, %(updated_at)s)
                            ON CONFLICT (session_id) DO NOTHING
                            """,
                            data
                        )
                except Exception as e:
                    logging.exception(
                        "DB insert failed: table=%s rows=%d",
                        table, len(data))
                    raise

                rows += len(data)

                # Update max_seen  
                for d in data: # Loop through each dictionary 
                    ts = parser.isoparse(d.get(ts_field)) # Get Field to use for watermark (usually updated_at) Converts text to datetime python.
                    if ts > max_seen: # max_seen = old watermark, everytime something more recent appears it updates max_seen.
                        max_seen = ts # max_seen will be equal to the biggest updated_at of all processed records.

            # Update watermark (only after the loop finishes) 
            max_seen_with_delta = max_seen + timedelta(microseconds=1)
            set_watermark(wm_name, max_seen_with_delta.isoformat()) # reConverts datetime to string ISO, and stores it into metadata table

            if table == "raw.customers":
                deleted_count = update_delete_flag(cur, "raw", "customers", 360)
                logging.info("Marked %d customers as deleted", deleted_count)

            final_msg = f"{rows} inserted" if rows > 0 else "No rows inserted"

            return final_msg
            

# Watermark use
# Last watermark stored = 2024-10-01T00:00:00Z
# In this execution you saw data until = 2024-11-18T14:32:10Z
# Watermark will be = 2024-11-18T14:32:10Z
# Next execution fetch_paged(endpoint, {"updated_since": "2024-11-18T14:32:10Z"})
# Only new data or changed records

def update_delete_flag(cur, schema, table, interval_days=180):
   
    sql = f"""
        UPDATE {schema}.{table} SET is_deleted = TRUE, updated_at = NOW()
        WHERE last_seen_at < now() - INTERVAL '{interval_days} days'
        AND is_deleted = FALSE;  -- only update records not already deleted
    """
    cur.execute(sql)
    return cur.rowcount


