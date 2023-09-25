from cognite.extractorutils import Extractor

from cdf_ts_replicator import __version__
from cdf_ts_replicator.config import Config
from cdf_ts_replicator.extractor import run_extractor


def main() -> None:
    with Extractor(
        name="cdf_ts_replicator",
        description="Stream datapoints from CDF to other destinations",
        config_class=Config,
        run_handle=run_extractor,
        version=__version__,
    ) as extractor:
        extractor.run()


if __name__ == "__main__":
    main()
