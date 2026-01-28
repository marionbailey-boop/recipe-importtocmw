from __future__ import annotations

from typing import List, Optional, Literal, Union
from pydantic import BaseModel, Field, model_validator, ConfigDict


class Ingredient(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sequence: float
    name: str
    amount: str
    unit: str
    notes: str


class MediaItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: str
    name: Optional[str]
    alt: str
    caption: Optional[str]
    width: Optional[float]
    height: Optional[float]
    format: Optional[str]
    size_bytes: Optional[float]
    type: Optional[str]
    step_index: Optional[float]
    attribution: Optional[str]
    license: Optional[str]
    copyright: Optional[str]
    seo_keywords: Optional[List[str]]
    created_at: Optional[str]
    uploaded_by: Optional[str]


class CalcmenuReference(BaseModel):
    model_config = ConfigDict(extra="forbid")

    recipe_number: str
    reference_id: str
    database_name: str
    code_site: str
    code_group: str


class RecipeJson(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str
    description: str
    servings: str
    prep_time: str
    cook_time: str
    total_time: str
    difficulty: Literal["easy", "medium", "hard"]
    cuisine: str
    category: str
    ingredients: List[Ingredient]
    instructions: List[str]
    dietary_tags: List[str]
    allergens: List[str]
    equipment: List[str]
    notes: str
    serving_suggestions: List[str]
    wine_pairing: str
    images: List[MediaItem]
    infographics: List[MediaItem]
    source_system: Literal["ai-generated"]
    calcmenu_reference: CalcmenuReference


class RecipeOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    response_plain: str
    is_recipe: bool
    # When is_recipe=false, upstream may send {}. When true, it must match RecipeJson.
    recipe_json: Union[RecipeJson, dict] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_conditional(self):
        if self.is_recipe:
            if not isinstance(self.recipe_json, RecipeJson):
                raise ValueError("recipe_json must be a full RecipeJson when is_recipe=true")
        else:
            # allow {} only when not recipe
            if not isinstance(self.recipe_json, dict) or self.recipe_json != {}:
                raise ValueError("recipe_json must be {} when is_recipe=false")
        return self
