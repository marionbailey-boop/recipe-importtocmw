from fastapi import FastAPI, HTTPException

app = FastAPI(title="Import Recipe into CMWeb", version="1.0.0")

def fetch_conn_str(apikey: str) -> str:
    r = requests.get("http://192.168.1.23:8006/get-connection-string", params={"apikey": apikey}, timeout=15)
    r.raise_for_status()
    return r.text.strip().strip('"')

# Connects to the database using the connection string obtained via the API key
def get_conn(apikey: str):
    conn_str = fetch_conn_str(apikey)

    return pyodbc.connect(conn_str, autocommit=True)

@app.get("/recipes/import-to-cmw")
def ImportRecipeIntoCMWEB(api_key:str, nooko_json: dict, translation: str):
    try: 
        conn_str = fetch_conn_str(apikey)

