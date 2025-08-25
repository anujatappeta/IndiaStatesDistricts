import cx_Oracle
import csv
import os

# Read DB credentials from environment variables
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")
DB_SERVICE = os.environ.get("DB_SERVICE")

dsn = cx_Oracle.makedsn(DB_HOST, DB_PORT, service_name=DB_SERVICE)
conn = cx_Oracle.connect(user=DB_USER, password=DB_PASSWORD, dsn=dsn)
cur = conn.cursor()

with open("state_districts.csv", "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    next(reader)  # skip header

    for state, district in reader:
        state = state.strip()
        district = district.strip()

        # Check if state exists
        cur.execute(
            "SELECT state_id FROM states WHERE LOWER(TRIM(state_name)) = :state",
            {"state": state.lower()}
        )
        row = cur.fetchone()

        if row:
            state_id = row[0]
        else:
            state_id_var = cur.var(cx_Oracle.NUMBER)
            cur.execute(
                "INSERT INTO states (state_name) VALUES (:state) RETURNING state_id INTO :id",
                {"state": state, "id": state_id_var}
            )
            state_id = int(state_id_var.getvalue()[0])

        # Check if district exists
        cur.execute(
            "SELECT district_id FROM districts WHERE LOWER(TRIM(district_name)) = :district AND state_id = :sid",
            {"district": district.lower(), "sid": state_id}
        )
        if not cur.fetchone():
            cur.execute(
                "INSERT INTO districts (district_name, state_id) VALUES (:district, :sid)",
                {"district": district, "sid": state_id}
            )

conn.commit()
print("Data inserted successfully without duplicates!")

cur.close()
conn.close()
