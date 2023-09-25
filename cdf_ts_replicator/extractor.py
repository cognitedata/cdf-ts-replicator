import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from typing import Dict, Union

from cognite.client import CogniteClient
from cognite.client.data_classes import TimeSeries
from cognite.client.data_classes.datapoints_subscriptions import DatapointSubscriptionBatch
from cognite.client.exceptions import CogniteAPIError
from cognite.extractorutils.configtools import CogniteConfig
from cognite.extractorutils.statestore import AbstractStateStore

from cdf_ts_replicator import __version__
from cdf_ts_replicator.config import Config, EventHubConfig


def run_extractor(client: CogniteClient, states: AbstractStateStore, config: Config, stop_event: Event) -> None:
    # init/connect to destination
    states.initialize()

    for subscription in config.subscriptions:
        logging.info(f"{client.time_series.subscriptions.retrieve(external_id=subscription.externalId)}")

    while not stop_event.is_set():
        start_time = time.time()  # Get the current time in seconds

        process_subscriptions(client, states, config, stop_event)

        end_time = time.time()  # Get the time after function execution
        elapsed_time = end_time - start_time
        sleep_time = max(5 - elapsed_time, 0)  # 900s = 15min
        if sleep_time > 0:
            time.sleep(sleep_time)


def process_subscriptions(client: CogniteClient, states: AbstractStateStore, config: Config, stop_event: Event) -> None:
    for subscription in config.subscriptions:
        for partition in subscription.partitions:
            with ThreadPoolExecutor() as executor:
                future = executor.submit(process_partition, client, subscription.externalId, partition, states, config)
                logging.info(future.result())


def process_partition(
    client: CogniteClient, external_id: str, partition: int, states: AbstractStateStore, config: Config
) -> str:
    state_id = f"{external_id}_{partition}"
    cursor = states.get_state(external_id=state_id)[1]
    logging.debug(f"{threading.get_native_id()} / {threading.get_ident()}: State for {state_id} is {cursor}")

    for update_batch in client.time_series.subscriptions.iterate_data(
        external_id=external_id, partition=partition, cursor=cursor, limit=config.extractor.batch_size
    ):
        for destination in config.destinations:
            submit_to_destination(destination, update_batch)

        states.set_state(external_id=state_id, high=update_batch.cursor)

        if not update_batch.has_next:
            return f"{state_id} no more data at {update_batch.cursor}"
    return "No new data"


def submit_to_destination(
    destination: Union[CogniteConfig, EventHubConfig], update_batch: DatapointSubscriptionBatch
) -> None:
    if type(destination) == EventHubConfig:
        send_to_eventhub(update_batch, destination)
    elif type(destination) == CogniteConfig:
        send_to_cdf(update_batch, destination)
    else:
        print("Unknown destination type")


def send_to_eventhub(update_batch: DatapointSubscriptionBatch, config: EventHubConfig) -> None:
    pass


def send_to_cdf(update_batch: DatapointSubscriptionBatch, config: CogniteConfig) -> None:
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
