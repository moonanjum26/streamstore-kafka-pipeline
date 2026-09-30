from uuid import uuid4
from models import Order
import pytest
from pydantic import ValidationError

def test_valid_order_passes():
    order = Order(order_id=uuid4(), user="Sofia", item="chicken bowl", quantity=3)
    assert order.quantity == 3

def test_negative_quantity_rejected():
    with pytest.raises(ValidationError):
        Order(order_id=uuid4(), user="Raj", item="burger", quantity=-1)

def test_empty_user_rejected():
    with pytest.raises(ValidationError):
        Order(order_id=uuid4(), user="   ", item="burger", quantity=1)

def test_discount_is_optional():
    order = Order(order_id=uuid4(), user="Raj", item="burger", quantity=2)
    assert order.discount is None

def test_missing_required_field_rejected():
    with pytest.raises(ValidationError):
        Order(order_id=uuid4(), user="Raj", item="burger")
