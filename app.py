from flask import Flask, jsonify, render_template
import cx_Oracle

app = Flask(__name__)

dsn = cx_Oracle.makedsn("localhost", 1521, service_name="ORCLPDB")
conn = cx_Oracle.connect(user="anuja", password="anuja123", dsn=dsn)

@app.route("/")
def home():
    return render_template("index.html")  # serves the HTML with dropdowns

@app.route("/states")
def get_states():
    cursor = conn.cursor()
    cursor.execute("""
        SELECT  STATE_ID, TRIM(STATE_NAME) AS STATE_NAME
        FROM states
        WHERE TRIM(STATE_NAME) IS NOT NULL
        ORDER BY TRIM(STATE_NAME)
    """)
    rows = cursor.fetchall()
    cursor.close()
    return jsonify([{"id": r[0], "name": r[1]} for r in rows])

@app.route("/districts/<int:state_id>")
def get_districts(state_id):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT  DISTRICT_ID, TRIM(DISTRICT_NAME) AS DISTRICT_NAME
        FROM districts
        WHERE STATE_ID = :sid
        ORDER BY TRIM(DISTRICT_NAME)
    """, sid=state_id)
    rows = cursor.fetchall()
    cursor.close()
    return jsonify([{"id": r[0], "name": r[1]} for r in rows])

if __name__ == "__main__":
    app.run(debug=True)
