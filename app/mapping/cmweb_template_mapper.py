from __future__ import annotations

from typing import List, Tuple
from app.schemas.nooko_recipe_output import RecipeJson

TemplateRow = Tuple[str, str, str, str, str, str, str, str]


def _row(c1="", c2="", c3="", c4="", c5="", c6="", c7="", c8="") -> TemplateRow:
    # Guarantee 8 text columns
    return (
        str(c1 or ""),
        str(c2 or ""),
        str(c3 or ""),
        str(c4 or ""),
        str(c5 or ""),
        str(c6 or ""),
        str(c7 or ""),
        str(c8 or ""),
    )


def map_nooko_recipe_to_cmweb_rows(recipe: RecipeJson) -> List[TemplateRow]:
    rows: List[TemplateRow] = []

    # --- Header block (fixed labels) ---
    rows.append(_row("Recipe", "Name", recipe.title, "", "", "", "", ""))

    # Nooko schema does not provide Recipe Number (calcmenu_reference must remain empty),
    # so we stage blank unless you later add a rule externally.
    rows.append(_row("", "Number", "", "", "", "", "", ""))

    # Yield: screenshot has qty in col3 and unit in col4. We map servings -> qty, unit fixed "serving".
    rows.append(_row("", "Yield", recipe.servings, "serving", "", "", "", ""))

    # Subrecipe: not in Nooko -> blank
    rows.append(_row("", "Subrecipe", "", "", "", "", "", ""))

    # Source: use source_system (ai-generated) to fill the Source line
    rows.append(_row("", "Source", recipe.source_system, "", "", "", "", ""))

    # Category
    rows.append(_row("", "Category", recipe.category, "", "", "", "", ""))

    # Remark: not in Nooko -> blank
    rows.append(_row("", "Remark", "", "", "", "", "", ""))

    # Description
    rows.append(_row("", "Description", recipe.description, "", "", "", "", ""))

    # Notes
    rows.append(_row("", "Notes", recipe.notes, "", "", "", "", ""))

    # Additional Notes: not in Nooko (separate) -> blank
    rows.append(_row("", "Additional Notes", "", "", "", "", "", ""))

    # Display Nutrition: always "Yes"
    rows.append(_row("", "Display Nutrition", "Yes", "", "", "", "", ""))

    # --- Ingredient section ---
    rows.append(
        _row(
            "", "Ingredient Name", "Number", "Quantity", "Unit", "Wastage", "Complement", "Preparation"
        )
    )

    for ing in recipe.ingredients:
        # Nooko has no ingredient number -> blank
        # Wastage default "0"
        # Complement blank
        # Preparation = ing.notes
        rows.append(_row("", ing.name, "", ing.amount, ing.unit, "0", "", ing.notes))

    # --- Procedure section ---
    rows.append(_row("", "Procedure"))

    for step in recipe.instructions:
        rows.append(_row("", step))

    return rows
