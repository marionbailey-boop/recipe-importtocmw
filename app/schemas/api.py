from pydantic import BaseModel, Field
from typing import Any, Dict, Optional
from enum import Enum


class TranslationEnum(str, Enum):
    English = "English"
    German = "German"
    French = "French"
    Italian = "Italian"
    Spanish = "Spanish"

class ConvertRequest(BaseModel):
    api_key: str
    translation: TranslationEnum
    nooko_json: Dict[str, Any]

class APIUsageCall(BaseModel):
    timestamp: str
    model: str
    module: str
    status: str
    input_tokens: int
    output_tokens: int
    cost_usd: float
    latency_ms: float
    error_message: Optional[str] = None
    error_type: Optional[str] = None

class APIUsage(BaseModel):
    calls: list[APIUsageCall] = Field(default_factory=list)

class ConvertResponse(BaseModel):
    success: bool
    message: str
    result: Dict[str, Any]
    API_Usage: APIUsage                