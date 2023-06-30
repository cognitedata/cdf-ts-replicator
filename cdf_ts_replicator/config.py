from dataclasses import dataclass
from typing import Dict, List, Optional

from cognite.extractorutils.rest.extractor import RestConfig


@dataclass
class SubscriptionsConfig:
    externalId: str
    partitions: List[int]

@dataclass
class Config(RestConfig):
    subscriptions: SubscriptionsConfig = SubscriptionsConfig