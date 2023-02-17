from dataclasses import dataclass
import string
import datetime

@dataclass
class Item:
    measurement: string
    tags: dict
    fields: dict
    timestamp: datetime

@dataclass
class orderInfo:
    amount: int
    price_per_unit: float
    orders: int

class BazaarItems:

    def __init__(self) -> None:
        pass

    def setData(self, data) -> None:
        self.__data = data

    def updateFields(self) -> int:
        for key, value in self._data["products"].items():
            itemcounter = 0

            buy_orders_size = 0
            sell_orders_size = 0
            for buy_orders in value["sell_summary"]:
                buy_orders_size = len(value["sell_summary"])
                amount = buy_orders['amount']
                priceperunit = buy_orders['pricePerUnit']
                orders = buy_orders['orders']

