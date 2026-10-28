import csv
import os

import psycopg2
import requests
from psycopg2 import sql
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = int(os.environ.get("DB_PORT", "5433"))
DB_PASSWORD = os.environ.get("DB_PASSWORD")
BASE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(BASE, "data", "raw", "sales.csv")
API = "https://data.wprdc.org/api/3/action/resource_show"
RESOURCE_ID = "5bbe6c55-bce6-4edb-9d04-68edeb6bf7b1"
EXPECTED_COLUMNS = (
    "_id",
    "PARID",
    "FULL_ADDRESS",
    "PROPERTYHOUSENUM",
    "PROPERTYFRACTION",
    "PROPERTYADDRESSDIR",
    "PROPERTYADDRESSSTREET",
    "PROPERTYADDRESSSUF",
    "PROPERTYADDRESSUNITDESC",
    "PROPERTYUNITNO",
    "PROPERTYCITY",
    "PROPERTYSTATE",
    "PROPERTYZIP",
    "SCHOOLCODE",
    "SCHOOLDESC",
    "MUNICODE",
    "MUNIDESC",
    "RECORDDATE",
    "SALEDATE",
    "PRICE",
    "DEEDBOOK",
    "DEEDPAGE",
    "SALECODE",
    "SALEDESC",
    "INSTRTYP",
    "INSTRTYPDESC",
)


def get_download_url():
    response = requests.get(API, params={"id": RESOURCE_ID}, timeout=60)
    response.raise_for_status()
    payload = response.json()
    if payload.get("success") is not True:
        raise RuntimeError("API response did not indicate success")
    return payload["result"]["url"]


def download(url):
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    temporary_path = f"{OUT}.tmp"
    response = requests.get(url, stream=True, timeout=300)
    try:
        response.raise_for_status()
        with open(temporary_path, "wb") as output:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    output.write(chunk)
        os.replace(temporary_path, OUT)
    except Exception:
        if os.path.exists(temporary_path):
            os.remove(temporary_path)
        raise
    finally:
        response.close()
    return OUT

def load(path):
    with open(path, newline=None, encoding="utf-8") as file:
        header = next(csv.reader(file))
        if header != list(EXPECTED_COLUMNS):
            raise RuntimeError(
                f"Unexpected CSV columns: expected {list(EXPECTED_COLUMNS)}, "
                f"got {header}"
            )
        columns = sql.SQL(", ").join(
            sql.SQL("{} text").format(sql.Identifier(column.lower()))
            for column in header
        )
        file.seek(0)

        with psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname="warehouse",
            user="praveen",
            password=DB_PASSWORD,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS raw")
                cur.execute(
                    sql.SQL("CREATE TABLE IF NOT EXISTS raw.sales ({})").format(
                        columns
                    )
                )
                cur.execute("SELECT count(*) FROM raw.sales")
                old = cur.fetchone()[0]
                cur.execute("TRUNCATE raw.sales")
                cur.copy_expert(
                    "COPY raw.sales FROM STDIN WITH (FORMAT csv, HEADER true)",
                    file,
                )
                cur.execute("SELECT count(*) FROM raw.sales")
                count = cur.fetchone()[0]
                if count == 0:
                    raise RuntimeError("raw.sales is empty after loading")
                if old > 0 and count < 0.9 * old:
                    raise RuntimeError(
                        f"Loaded row count dropped from {old} to {count}"
                    )

    return count


if __name__ == "__main__":
    url = get_download_url()
    print("downloading", url)
    path = download(url)
    print("saved", path)
    print("loaded", load(path), "rows")
