from pydantic import BaseModel, field_validator
from uuid import UUID
from typing import Optional

class Order(BaseModel):
    order_id: UUID
    user: str
    item: str
    quantity: int
    discount: Optional[float] = None

    @field_validator("quantity")
    @classmethod
    def validate_quantity(cls, v):
        if v <= 0:
            raise ValueError("Quantity must be positive")
        return v

    @field_validator("user", "item")
    @classmethod
    def must_not_be_empty(cls, v):
        if not v.strip():
            raise ValueError("field must not be empty")
        return v
