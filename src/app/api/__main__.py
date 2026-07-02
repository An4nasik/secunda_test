"""API service entrypoint: ``python -m app.api``."""

import uvicorn


def main() -> None:
    """Run uvicorn with logging delegated to the application configuration."""
    uvicorn.run(
        "app.api.app:create_app",
        factory=True,
        host="0.0.0.0",  # noqa: S104 - binds inside the container
        port=8000,
        log_config=None,
    )


if __name__ == "__main__":
    main()
