"""Consumer entrypoint: ``python -m app.consumer``."""

import asyncio

from app.consumer.app import create_app


def main() -> None:
    """Run the FastStream consumer application."""
    asyncio.run(create_app().run())


if __name__ == "__main__":
    main()
