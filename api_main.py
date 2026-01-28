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

@app.post("/recipes/import/nooko-to-cmweb")
def import_recipe(payload: RecipeOutput):
    if not payload.is_recipe:
        return {"imported": False, "reply": payload.response_plain}

    recipe: RecipeJson = payload.recipe_json
    rows = map_nooko_recipe_to_cmweb_rows(recipe)

    with get_connection() as conn:
        id_main = import_nooko_rows_to_cmweb(
            conn=conn,
            rows=rows,
            file_name="recipes to import TEST",
            code_site=1,
            code_user=1,
            site_language=1,
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


# Convert Nooko to CMWeb, Call Benj SP 
@app.post("/recipes/import/nooko-to-cmw", response_model=ConvertResponse)
def recipe_convert_into_cmc(req: ConvertRequest):
    usage = APIUsage(calls=[])

    try:

        # Nooko to CMW

        # Import Converted Json to Benj by calling SP

        return ConvertResponse(
            success=True,
            message="Mapped and imported successfully.",
            result={},
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


