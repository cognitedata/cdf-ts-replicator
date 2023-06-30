
from dataclasses import dataclass
from typing import List, Union

@dataclass
class TimeSeriesId:
    id: int
    externalId: str

@dataclass
class TimeSeriesUpdate:
    timestamp: int
    value: Union[int, str]

@dataclass
class TimeSeriesDelete:
    inclusiveBegin: int
    exclusiveEnd: int

@dataclass
class TimeSeriesUpdate:
    timeSeries: TimeSeriesId
    upserts: List[TimeSeriesUpdate]
    deletes: List[TimeSeriesDelete]

@dataclass
class SubscriptionChange:
    added: List[TimeSeriesId]
    removed: List[TimeSeriesId]

@dataclass
class Partition:
    index: int
    nextCursor: str

@dataclass
class SubscriptionsDataList:
    updates: List[TimeSeriesUpdate]
    subscriptionChanges: List[SubscriptionChange]
    partitions: List[Partition]
    hasNext: bool