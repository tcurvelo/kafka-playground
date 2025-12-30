from dataclasses import dataclass
from typing import Optional

from dataclasses_avroschema import AvroModel


@dataclass
class BalanceKey(AvroModel):
    account_id: str


@dataclass
class BalanceV1(AvroModel):
    account_id: str
    balance: int

    class Meta:
        name = "balance"


@dataclass
class BalanceV2(AvroModel):
    account_id: str
    balance: int
    currency: Optional[str] = "USD"

    class Meta:
        name = "balance"
