import os, time, requests, logging
from datetime import datetime, timedelta
from dateutil import parser
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

API_BASE = os.environ.get("API_BASE_URL", "http://mock-api:8000")
API_KEY = os.environ["API_KEY"]
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

def _get_watermark(name, default_iso):
    return Variable.get(name, default_var=default_iso)

def _set_watermark(name, iso):
    Variable.set(name, iso)

def _fetch_paged(endpoint, params, max_retries=5): #Endpoint (Customers, Sessions), Params(filters, dates), Max retries number if request fails
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
        


        if data.get("next_page"): # If there is a next page, update page and while loop continues
            page = data["next_page"]
        else:
            return