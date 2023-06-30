
import arrow

from cognite.extractorutils.uploader_types import Event
from cognite.extractorutils.rest.extractor import RestExtractor

from cdf_ts_replicator import __version__
from cdf_ts_replicator.dto import SubscriptionsDataList
from cdf_ts_replicator.config import Config




class TimeSeriesExtractor(RestExtractor):

    def run(self) -> None:

        config: Config = extractor.get_current_config()

        payload = {
            "externalId": config.subscriptions.externalId,
            "partitions": [ { "index": partition, "cursor": None} for partition in config.subscriptions.partitions ]
        }
        
        @extractor.post(f"{config.source.base_url}timeseries/subscriptions/data/list", body=payload, headers={"cdf-version": "beta"}, response_type=SubscriptionsDataList)
        def get_data(data: SubscriptionsDataList):# -> Generator[Event, None, None]:
            for update in data.updates:
                print(update)


extractor = TimeSeriesExtractor(
    name="cdf_ts_replicator",
    description="Stream time series from one CDF project to another using time series subscriptions",
    version=__version__,
    config_class=Config,
)