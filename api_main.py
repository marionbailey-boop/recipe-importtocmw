from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException
from pydantic_core import ValidationError as PydanticValidationError

from db.session import db_cursor
from db.connection import CONNECTION_STRING

from app.schemas.api import ConvertRequest, ConvertResponse, APIUsage
from app.utils.errors import ValidationError as AppValidationError, MappingError, DownstreamError

from app.schemas.nooko_recipe_output import RecipeOutput, RecipeJson
from app.mapping.cmweb_template_mapper import map_nooko_recipe_to_cmweb_rows
from app.services.cmweb_import_service import import_nooko_rows_to_cmweb
from db.connection import get_connection  # or your existing DB getter


SERVICE_ID = "recipe-convert-into-cmweb"

app = FastAPI(
    title="Recipe Import (Nooko into CMWeb)",
    version="1.0.0",
)

def _import_recipe_into_cmweb(
    payload: RecipeOutput,
    *,
    api_key: Optional[str] = None,
    file_name: str = "recipes to import TEST",
    code_site: int = 1,
    code_user: int = 1,
    site_language: int = 1,
) -> Dict[str, Any]:
    if not payload.is_recipe:
        return {"imported": False, "reply": payload.response_plain}

    recipe: RecipeJson = payload.recipe_json
    rows = map_nooko_recipe_to_cmweb_rows(recipe)

    with get_connection(api_key) as conn:
        id_main = import_nooko_rows_to_cmweb(
            conn=conn,
            rows=rows,
            file_name=file_name,
            code_site=code_site,
            code_user=code_user,
            site_language=site_language,
        )

    return {"imported": True, "idMain": id_main, "staged_rows": len(rows)}


@app.get("/")
def read_root():
    return {"service": SERVICE_ID, "status": "running"}


@app.get("/health")
def health():
    try:
        with db_cursor() as cursor:
            cursor.execute("SELECT 1")
        return {"status": "healthy"}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"status": "unhealthy", "error": str(e)},
        )


# Main endpoint aligned with other/api_main.py
@app.post("/ImportRecipeIntoCMWEB", response_model=ConvertResponse)
def ImportRecipeIntoCMWEB(req: ConvertRequest):
    usage = APIUsage(calls=[])

    try:
        payload = RecipeOutput.model_validate(req.nooko_json)
        result = _import_recipe_into_cmweb(payload, api_key=req.api_key)

        message = "Imported successfully." if result.get("imported") else "No recipe to import."
        return ConvertResponse(
            success=True,
            message=message,
            result=result,
            API_Usage=usage,
        )

    except (PydanticValidationError, AppValidationError, MappingError) as e:
        raise HTTPException(
            status_code=400,
            detail={"error": str(e), "API_Usage": usage.model_dump()},
        )

    except DownstreamError as e:
        raise HTTPException(
            status_code=502,
            detail={"error": str(e), "API_Usage": usage.model_dump()},
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error": str(e), "API_Usage": usage.model_dump()},
        )
