"""Entry point: python -m a2a_bridge"""
import uvicorn

from .app import create_app
from .config import load_settings


def main() -> None:
    s = load_settings()
    uvicorn.run(create_app(s), host=s.host, port=s.port, workers=1, proxy_headers=s.trust_proxy_headers,
                forwarded_allow_ips="*" if s.trust_proxy_headers else None, log_config=None,
                timeout_graceful_shutdown=10)


if __name__ == "__main__":
    main()
