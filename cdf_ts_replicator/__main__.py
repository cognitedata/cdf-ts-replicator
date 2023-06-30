
from cdf_ts_replicator import __version__
from cdf_ts_replicator.extractor import extractor


def main() -> None:
    with extractor:
        extractor.run()


if __name__ == "__main__":
    main()
