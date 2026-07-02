"""Outbox relay entrypoint: ``python -m app.outbox``."""

from app.outbox.relay import main

if __name__ == "__main__":
    main()
