import logging

import collector


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    collector.run()


if __name__ == "__main__":
    main()
