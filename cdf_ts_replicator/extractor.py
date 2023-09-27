import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from typing import Any, Dict, List, Union

from azure.eventhub import EventData, EventHubProducerClient
from azure.eventhub.exceptions import EventHubError
from cognite.client import CogniteClient
from cognite.client.data_classes import TimeSeries
from cognite.client.data_classes.datapoints_subscriptions import DatapointSubscriptionBatch
from cognite.client.exceptions import CogniteAPIError
from cognite.extractorutils.base import Extractor
from cognite.extractorutils.configtools import CogniteConfig
from cognite.extractorutils.metrics import BaseMetrics
from cognite.extractorutils.statestore import AbstractStateStore
from cognite.extractorutils.util import retry

from cdf_ts_replicator import __version__
from cdf_ts_replicator.config import Config, EventHubConfig
from cdf_ts_replicator.metrics import Metrics


class TimeSeriesReplicator(Extractor):
    def __init__(self, metrics: Metrics, stop_event: Event) -> None:
        super().__init__(
            name="cdf_ts_replicator",
            description="CDF Timeseries Replicator",
            config_class=Config,
            metrics=metrics,
            use_default_state_store=False,
            version=__version__,
            cancellation_token=stop_event,
        )
        self.metrics: Metrics
        self.stop_event = stop_event
        self.endpoint_source_map: Dict[str, Any] = {}
        self.errors: List[str] = []

    def run(self) -> None:
        # init/connect to destination
        self.state_store.initialize()

        for subscription in self.config.subscriptions:
            logging.info(
                f"{self.cognite_client.time_series.subscriptions.retrieve(external_id=subscription.externalId)}"
            )

        while not self.stop_event.is_set():
            start_time = time.time()  # Get the current time in seconds

            self.process_subscriptions()

            end_time = time.time()  # Get the time after function execution
            elapsed_time = end_time - start_time
            sleep_time = max(5 - elapsed_time, 0)  # 900s = 15min
            if sleep_time > 0:
                time.sleep(sleep_time)

    def process_subscriptions(self) -> None:
        for subscription in self.config.subscriptions:
            for partition in subscription.partitions:
                with ThreadPoolExecutor() as executor:
                    future = executor.submit(self.process_partition, subscription.externalId, partition)
                    logging.info(future.result())

    def process_partition(self, external_id: str, partition: int) -> str:
        state_id = f"{external_id}_{partition}"
        cursor = self.state_store.get_state(external_id=state_id)[1]
        logging.debug(f"{threading.get_native_id()} / {threading.get_ident()}: State for {state_id} is {cursor}")

        for update_batch in self.cognite_client.time_series.subscriptions.iterate_data(
            external_id=external_id, partition=partition, cursor=cursor, limit=self.config.extractor.batch_size
        ):
            for destination in self.config.destinations:
                self.submit_to_destination(destination, update_batch)

            self.state_store.set_state(external_id=state_id, high=update_batch.cursor)

            if not update_batch.has_next:
                return f"{state_id} no more data at {update_batch.cursor}"
        return "No new data"

    def submit_to_destination(
        self, destination: Union[CogniteConfig, EventHubConfig], update_batch: DatapointSubscriptionBatch
    ) -> None:
        if type(destination) == EventHubConfig:
            self.send_to_eventhub(update_batch, destination)
        elif type(destination) == CogniteConfig:
            self.send_to_cdf(update_batch, destination)
        else:
            print("Unknown destination type")

    def _get_producer(self, connection_string: str, eventhub_name: str) -> Union[EventHubProducerClient, None]:
        if connection_string == None or eventhub_name == None:
            return None

        return EventHubProducerClient.from_connection_string(conn_str=connection_string, eventhub_name=eventhub_name)

    def send_to_eventhub(self, update_batch: DatapointSubscriptionBatch, config: EventHubConfig) -> None:
        producer = self._get_producer(config.connection_string, config.eventhub_name)

        if producer:
            with producer:
                try:
                    event_data_batch = producer.create_batch(max_size_in_bytes=config.event_hub_batch_size)
                    jsonLines = ""
                    for update in update_batch.updates:
                        for i in range(0, len(update.upserts.timestamp)):
                            try:
                                jsonData = json.dumps(
                                    {
                                        "externalId": update.upserts.external_id,
                                        "timestamp": update.upserts.timestamp[i],
                                        "value": update.upserts.value[i],
                                    },
                                )

                                if not config.use_jsonl:
                                    event_data_batch.add(EventData(jsonData))
                                else:
                                    jsonLines = jsonLines + f"\n{jsonData}"
                                    if len(jsonLines.split("\n")) == config.jsonl_batch_size:
                                        event_data_batch.add(EventData(jsonLines))
                                        jsonLines = ""

                            except ValueError:
                                # EventDataBatch object reaches max_size.
                                logging.info(f"Length {len(jsonLines)}")
                                logging.info(f"X Send batch {len(event_data_batch)}")
                                producer.send_batch(event_data_batch)
                                event_data_batch = producer.create_batch(max_size_in_bytes=config.event_hub_batch_size)
                                if not config.use_jsonl:
                                    event_data_batch.add(EventData(jsonData))
                                elif config.use_jsonl:
                                    event_data_batch.add(EventData(jsonLines))
                                    jsonLines = ""

                    event_data_batch.add(EventData(jsonLines))
                    logging.info(f"Y Send batch {len(event_data_batch)}")
                    producer.send_batch(event_data_batch)
                except EventHubError as eh_err:
                    logging.warning("Sending error: ", eh_err)

    def send_to_cdf(self, update_batch: DatapointSubscriptionBatch, config: CogniteConfig) -> None:
        dst_client = config.get_cognite_client(client_name="ts-replicator")

        try:
            dps: Dict[str, list] = {}
            for update in update_batch.updates:
                xid = config.external_id_prefix + update.upserts.external_id
                if not xid in dps:
                    dps[xid] = []
                for i in range(0, len(update.upserts.timestamp)):
                    dps[xid].append((update.upserts.timestamp[i], update.upserts.value[i]))

            ingest_dps = [{"external_id": external_id, "datapoints": dps[external_id]} for external_id in dps]
            dst_client.time_series.data.insert_multiple(ingest_dps)

        except CogniteAPIError as err:
            for update in update_batch.updates:
                try:
                    dst_client.time_series.create(
                        TimeSeries(external_id=update.upserts.external_id, name=update.upserts.external_id)
                    )
                except CogniteAPIError as err:
                    print(err)

            dst_client.time_series.data.insert_multiple(ingest_dps)
