from typing import List, Dict
from decimal import Decimal
from datetime import datetime

class Ride:
    def __init__(self, arr: List[str]):
        self.lpep_pickup_datetime = datetime.strptime(arr[1], "%Y-%m-%d %H:%M:%S"),
        self.lpep_dropoff_datetime = datetime.strptime(arr[2], "%Y-%m-%d %H:%M:%S"),
        self.pu_location_id = int(arr[5])
        self.do_location_id = int(arr[6])
        self.passenger_count = int(arr[7]) if arr[7] else 0
        self.trip_distance = Decimal(arr[8])
        self.tip_amount = Decimal(arr[12])
        
    @classmethod
    def from_dict(cls, d: Dict):
        return cls(arr=[
            d['lpep_pickup_datetime'][0],
            d['lpep_dropoff_datetime'][0],
            d['pu_location_id'],
            d['do_location_id'],
            d['passenger_count'],
            d['trip_distance'],
            d['tip_amount'],
        ]
        )

    def __repr__(self):
        return f'{self.__class__.__name__}: {self.__dict__}'