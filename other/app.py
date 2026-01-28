import os
import json
import time
from typing import Any, Dict, List, Optional

import pyodbc
from dotenv import load_dotenv
import requests
from fastapi import FastAPI, Query
from fastapi.responses import RedirectResponse, JSONResponse

load_dotenv()

SERVICE_ID = os.getenv("SERVICE_ID", "nooko-product-api")

app = FastAPI(
    title="Nooko CMWeb → Nooko Product Service",
    docs_url="/docs",
    openapi_url="/openapi.json",
)

# Wrapper for the response 
def envelope(
    reply: str,
    *,
    data: Optional[Any] = None,
    latency_ms: int = 0,
    input_tokens: int = 0,
    output_tokens: int = 0,
    model: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    total_tokens = int(input_tokens) + int(output_tokens)
    return {
        "reply": reply,
        "data": data,
        "metadata": {
            "usage": {
                "input_tokens": int(input_tokens),
                "output_tokens": int(output_tokens),
                "total_tokens": int(total_tokens),
            },
            "model": model,
            "latency_ms": int(latency_ms),
            "config": config,
        },
        "service_id": SERVICE_ID,
    }

# Wrapper for the payload structure
def spec_payload(
    *,
    success: bool,
    message: str,
    data: Optional[Any] = None,
    errors: Optional[Any] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "success": bool(success),
        "message": message,
        "data": data if data is not None else [],
    }
    if errors is not None:
        payload["errors"] = errors
    return payload

# Fetches the connection string from the API using the provided API key
def fetch_conn_str(apikey: str) -> str:
    r = requests.get("http://192.168.1.23:8006/get-connection-string", params={"apikey": apikey}, timeout=15)
    r.raise_for_status()
    return r.text.strip().strip('"')

# Connects to the database using the connection string obtained via the API key
def get_conn(apikey: str):
    conn_str = fetch_conn_str(apikey)

    return pyodbc.connect(conn_str, autocommit=True)

# Tries and tests the database connection
def try_connect(conn_str: str) -> tuple[bool, str | None]:
    try:
        with pyodbc.connect(conn_str, timeout=15, autocommit=True) as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
        return True, None
    except Exception as e:
        return False, str(e)

# Generates a standardized database error response
def db_error_response(step_name: str, t0: float, e: Exception):
    latency_ms = int((time.perf_counter() - t0) * 1000)
    return JSONResponse(
        status_code=500,
        content=envelope(
            f"{step_name.title()} failed",
            data=spec_payload(
                success=False,
                message="Database operation failed",
                data=[],
                errors={"code": "DB_ERROR", "detail": str(e)},
            ),
            latency_ms=latency_ms,
            config={"step": step_name},
        ),
    )

# Generates a standardized JSON parse error response
def json_parse_error_response(step_name: str, t0: float):
    latency_ms = int((time.perf_counter() - t0) * 1000)
    return JSONResponse(
        status_code=500,
        content=envelope(
            f"{step_name.title()} failed",
            data=spec_payload(
                success=False,
                message="Stored procedure returned invalid JSON",
                data=[],
                errors={"code": "INVALID_JSON"},
            ),
            latency_ms=latency_ms,
            config={"step": step_name},
        ),
    )

# Health check endpoint
@app.get("/health", include_in_schema=False)
def health():
    return {"status": "ok"}

# Root endpoint redirects to docs
@app.get("/")
def root():
    return RedirectResponse(url="/docs")

# Connection validation endpoint (ENDPOINT A)
@app.post("/cmweb/products/connection/validate")
def validate_cmweb_connection(apikey: str = Query(...)):
    t0 = time.perf_counter()
    try:
        conn_str = fetch_conn_str(apikey)

        ok, err = try_connect(conn_str)
        latency_ms = int((time.perf_counter() - t0) * 1000)

        if not ok:
            return JSONResponse(
                status_code=400,
                content=envelope(
                    "CMWeb connection validation failed",
                    data=spec_payload(
                        success=False,
                        message="Connection failed",
                        data=[],
                        errors={"connection": err},
                    ),
                    latency_ms=latency_ms,
                    config={"step": "connection_validate"},
                ),
            )

        return envelope(
            "CMWeb connection validated",
            data=spec_payload(
                success=True,
                message="Connection validated successfully",
                data={"status": "ok"},
            ),
            latency_ms=latency_ms,
            config={"step": "connection_validate"},
        )

    except Exception as e:
        latency_ms = int((time.perf_counter() - t0) * 1000)
        return JSONResponse(
            status_code=500,
            content=envelope(
                "CMWeb connection validation failed",
                data=spec_payload(
                    success=False,
                    message="Validation error",
                    data=[],
                    errors={"code": "VALIDATION_ERROR"},
                ),
                latency_ms=latency_ms,
                config={"step": "connection_validate"},
            ),
        )


# Function that exports products from CMWeb using the stored procedure
def run_export(
    *,
    Translation: str,
    CodeNutrientSet: int | None,
    CodeSetPrice: int | None, 
    CodeSite: int,
    apikey: str,
    step_name: str = "export",
    AfterCode: int | None = Query(default=None, ge=1),
    PageSize: int = Query(default=100, ge=1, le=1000)

) -> Any:
    t0 = time.perf_counter()

    sql = """
    SET NOCOUNT ON;
    EXEC dbo.Nooko_GetProduct
        @Translation = ?,
        @CodeNutrientSet = ?,
        @CodeSetPrice = ?,
        @CodeSite = ?,
        @AfterCode = ?,
        @PageSize = ?;
    """
    
    try:
        with get_conn(apikey) as conn:
            
            cur = conn.cursor()
            
            cur.execute(
                sql,
                Translation,
                CodeNutrientSet,
                CodeSetPrice,
                CodeSite,
                AfterCode,
                PageSize,
            )

            row = cur.fetchone()
            latency_ms = int((time.perf_counter() - t0) * 1000)

            # Returns an empty result set
            if not row or row[0] is None:
                return envelope(
                    f"{step_name.title()} completed (0 records)",
                    data=spec_payload(
                        success=True,
                        message=f"{step_name.title()} completed with 0 records",
                        data=[],
                    ),
                    latency_ms=latency_ms,
                    config={"step": step_name, "stored_procedure": "dbo.Nooko_GetProduct"},
                )

            json_text = row[0]
            exported = json.loads(json_text) if isinstance(json_text, str) else json_text

            # ALWAYS normalize to list
            if exported is None:
                exported_list: List[Any] = []
            elif isinstance(exported, list):
                exported_list = exported
            else:
                exported_list = [exported]

            next_after_code = None
            if exported_list:
                last = exported_list[-1]
                if isinstance(last, dict):
                    next_after_code = last.get("code")

            return envelope(
                f"{step_name.title()} completed",
                data=spec_payload(
                    success=True,
                    message=f"{step_name.title()} completed with {len(exported_list)} record(s)",
                    data=exported_list,  # <-- always array
                ),
                latency_ms=latency_ms,
                config={
                    "step": step_name,
                    "stored_procedure": "dbo.Nooko_GetProduct",
                    "params": {
                        "Translation": Translation,
                        "CodeNutrientSet": CodeNutrientSet,
                        "CodeSetPrice": CodeSetPrice,
                        "CodeSite": CodeSite,
                        "AfterCode": AfterCode,
                        "PageSize": PageSize,
                    },
                    "cursor": {
                        "AfterCode": AfterCode,
                        "PageSize": PageSize,
                        "NextAfterCode": next_after_code,
                    }
                },
            )

    # Database error
    except pyodbc.Error as e:
        return db_error_response(step_name, t0, e)

    # JSON parsing error
    except json.JSONDecodeError:
        return json_parse_error_response(step_name, t0)

    # Unexpected error
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content=envelope(
                f"{step_name.title()} failed",
                data=spec_payload(
                    success=False,
                    message="Unexpected error",
                    data=[],
                    errors={"code": "UNEXPECTED_ERROR", "detail": str(e)},
                ),
                latency_ms=int((time.perf_counter() - t0) * 1000),
                config={"step": step_name},
            ),
        )

@app.post("/cmweb/products/export")
@app.get("/cmweb/products/export")
def export_products_from_cmweb_get(
    apikey: str = Query(...),
    CodeSite: int = Query(..., ge=1),
    Translation: str = Query(...),
    CodeNutrientSet: int | None = Query(default=None),
    CodeSetPrice: int | None = Query(default=None),
    AfterCode: int | None = Query(default=None),
    PageSize: int = Query(default=100, ge=1, le=1000)

    # REMOVED
    # Locale: str | None = Query(), 
    # DateFormat: str | None = Query(),
    # TimeFormat: str | None = Query(),
    # NumberFormat: str | None = Query(),
    # Currency: str | None = Query(),

    # TO ADD
    # apikey: str | None = Header(), REQUIRED - to get the connection string from the API key
    # translation: str | None = Header(), REQUIRED
    # codesetprice : int | None = Header(), OPTIONAL
    # codesite : int | None = Header(), REQUIRED

):
    return run_export(
        Translation=Translation,
        CodeNutrientSet=CodeNutrientSet,
        CodeSetPrice=CodeSetPrice,
        CodeSite=CodeSite,
        apikey=apikey,
        AfterCode=AfterCode,
        PageSize=PageSize,
        step_name="export",

        # REMOVED
        # Locale=Locale,
        # DateFormat=DateFormat,
        # TimeFormat=TimeFormat,
        # NumberFormat=NumberFormat,
        # Currency=Currency,
    )

@app.post("/cmweb/products/map-to-nooko")
def map_to_nooko(
    apikey: str = Query(...),
    CodeSite: int = Query(..., ge=1),
    Translation: str = Query(...),
    CodeNutrientSet: int | None = Query(default=None),
    CodeSetPrice: int | None = Query(default=None),
    AfterCode: int | None = Query(default=None),
    PageSize: int = Query(default=100, ge=1, le=1000)
):
    return run_export(
        Translation=Translation,
        CodeNutrientSet=CodeNutrientSet,
        CodeSetPrice=CodeSetPrice,
        CodeSite=CodeSite,
        apikey=apikey,
        AfterCode=AfterCode,
        PageSize=PageSize,
        step_name="map_to_nooko",
    )

@app.post("/nooko/products/import")
def import_products_to_nooko(body: Optional[Dict[str, Any]] = None):
    t0 = time.perf_counter()
    latency_ms = int((time.perf_counter() - t0) * 1000)

    return JSONResponse(
        status_code=501,
        content=envelope(
            "Import not implemented",
            data=spec_payload(
                success=False,
                message="Import endpoint is not implemented yet",
                data=[],
                errors={"import": "Not implemented"},
            ),
            latency_ms=latency_ms,
            config={"step": "import", "status": "not_implemented"},
        ),
    )
