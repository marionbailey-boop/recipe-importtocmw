from __future__ import annotations
from urllib.parse import urlparse
from typing import Any, Dict, List
import pyodbc


def map_nooko_to_cmc(nooko_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Maps Nooko recipe payload(s) into CMC payload list.
    Input supports:
      - single recipe object (like single_nooko_recipe.json)
      - multi export wrapper with recipes[].content (like multiple_nooko_recipe.json)

    Output:
      - always List[dict] where each dict matches cmc_recipe.json structure.
    """
    nooko_recipes = _extract_nooko_recipe_contents(nooko_json)
    return [_map_one_recipe(r) for r in nooko_recipes]


# ----------------------------
# Extractors (single vs multi)
# ----------------------------
def _extract_nooko_recipe_contents(nooko_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    # multiple_nooko_recipe.json shape: {"recipes":[{"content":{...}}, ...]}
    recipes = nooko_json.get("recipes")
    if isinstance(recipes, list):
        out: List[Dict[str, Any]] = []
        for item in recipes:
            if isinstance(item, dict) and isinstance(item.get("content"), dict):
                out.append(item["content"])
        return out

    # some wrappers may be {"content": {...}}
    if isinstance(nooko_json.get("content"), dict):
        return [nooko_json["content"]]

    # single_nooko_recipe.json shape: recipe object
    return [nooko_json]


# ----------------------------
# Main mapper (Nooko -> CMC)
# ----------------------------
def _map_one_recipe(r: Dict[str, Any]) -> Dict[str, Any]:
    title = _as_str(r.get("title"))
    description = _as_str(r.get("description"))
    category = _as_str(r.get("category"))
    source_system = _as_str(r.get("source_system"))

    cmc: Dict[str, Any] = {
        # ---- Recipe header ----
        "RecipeNumber": "",                 # CMC-generated / optional
        "RecipeName": title,                # obvious match: title -> RecipeName
        "AlternativeName": "",
        "Category": category,               # obvious match: category -> Category
        "Source": source_system,            # best available match: source_system -> Source
        "Author": "",
        "RecipeImage": "",                  # filled after Chester (filename/id)
        "PictureName": "",                  # filled after Chester (filename/id list)
        "isSubRecipe": False,
        "srQty": 0.0,
        "srUnit": "",

        # ---- Yield / description ----
        "Yield": {
            "YieldQty": _as_float(r.get("servings")),  # servings is string in sample -> float
            "YieldUnit": "serving",                    # default; adjust if your CMC expects another unit
        },
        "Description": {"Description": description},   # obvious match: description -> Description.Description
        "ServingSize": {
            "ServingAmount": 0.0,                      # no direct field in Nooko examples
            "ServingUnit": "",
        },

        # ---- Body ----
        "Ingredients": _map_ingredients(r.get("ingredients")),
        "Procedure": _map_procedure(r.get("instructions")),
    }

    return cmc


# ----------------------------
# Sub-mappers
# ----------------------------
def _map_ingredients(ings: Any) -> List[Dict[str, Any]]:
    if not isinstance(ings, list):
        return []

    out: List[Dict[str, Any]] = []
    for ing in ings:
        if not isinstance(ing, dict):
            continue

        seq = ing.get("sequence")  # int in sample
        name = _as_str(ing.get("name"))
        qty = _as_float(ing.get("amount"))  # amount is string in sample -> float
        unit = _as_str(ing.get("unit"))
        notes = _as_str(ing.get("notes"))

        out.append(
            {
                "Number": _as_str(seq),          # CMC expects string
                "Name": name,                   # obvious match: name -> Name
                "Unit": unit,                   # obvious match: unit -> Unit
                "Quantity": qty,                # obvious match: amount -> Quantity (float)
                "Complement": notes,            # best-fit: notes -> Complement
                "Price": 0.0,
                "Amount": 0.0,
                "Wastage1": 0.0,
                "Wastage2": 0.0,
                "Wastage3": 0.0,
                "Wastage4": 0.0,
                "Wastage5": 0.0,
                "showIngredientPercentage": False,
                "codeAccounting": 0,
                "preparation": "",              # no direct field in Nooko examples
                "AlternativeName": "",
            }
        )

    return out


def _map_procedure(steps: Any) -> List[Dict[str, Any]]:
    if not isinstance(steps, list):
        return []

    out: List[Dict[str, Any]] = []
    for idx, step in enumerate(steps, start=1):
        out.append(
            {
                "Step": str(idx),               # CMC expects string
                "Instruction": _as_str(step),   # obvious match: instructions[i] -> Procedure[i].Instruction
            }
        )
    return out


# ----------------------------
# Type helpers (keep strict)
# ----------------------------
def _as_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    return str(v).strip()


def _as_float(v: Any) -> float:
    """
    Converts numeric-like values to float.
    Examples in your Nooko files: "4", "600", "10" -> float.
    Non-numeric -> 0.0
    """
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


# ----------------------------
# Inject translation into cmc format
# ----------------------------
def attach_translation(cmc_recipes: List[Dict[str, Any]], translation: str) -> List[Dict[str, Any]]:
    """
    Returns a NEW list with translation injected into each CMC payload.
    Keeps mapper pure and makes QA conversion-only testing easy.
    """
    t = str(translation).strip()
    out: List[Dict[str, Any]] = []
    for r in cmc_recipes:
        copy = dict(r)
        copy["translation"] = t
        out.append(copy)
    return out


# ----------------------------
# Build Import payload
# ----------------------------
def build_import_payload(api_key: str, cmc_recipes: List[Dict[str, Any]]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"api_key": str(api_key).strip()}

    for idx, recipe in enumerate(cmc_recipes, start=1):
        payload[f"converted_recipe-{idx}"] = recipe

    return payload


