from src.stream.generator import make_user_id, amount_gbp
import random


def test_make_user_id_format():
    uid = make_user_id(5)
    assert uid.startswith("user_")


def test_amount_range():
    rng = random.Random(0)
    amount = amount_gbp(rng)

    assert amount > 0
    assert amount < 2000