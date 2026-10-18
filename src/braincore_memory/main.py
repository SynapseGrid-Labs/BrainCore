import uvicorn

from .config import get_settings


def main() -> None:
    settings = get_settings()
    uvicorn.run("braincore_memory.api:app", host=settings.bind_host, port=settings.port)


if __name__ == "__main__":
    main()
