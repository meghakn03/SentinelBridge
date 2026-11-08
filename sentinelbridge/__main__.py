import argparse
import os
import signal
import subprocess
import sys
import time


def port_number(value: str) -> int:
    port = int(value)
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("port must be between 1 and 65535")
    return port


def serve(module: str, port: int, env: dict) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-m", "uvicorn", module, "--host", "127.0.0.1",
         "--port", str(port), "--no-access-log"],
        env=env,
        start_new_session=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Start the SentinelBridge dashboard and simulator.")
    parser.add_argument("--port", type=port_number, default=8000, help="dashboard port (default: 8000)")
    parser.add_argument("--simulator-port", type=port_number, default=8001, help="simulator port (default: 8001)")
    args = parser.parse_args()
    if args.port == args.simulator_port:
        parser.error("dashboard and simulator ports must differ")

    env = os.environ.copy()
    simulator = f"http://127.0.0.1:{args.simulator_port}"
    env.setdefault("SOURCE_URL", f"{simulator}/events")
    env.setdefault("SPLUNK_HEC_URL", f"{simulator}/services/collector/event")

    def stop(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, stop)
    services = []
    try:
        services.append(serve("sentinelbridge.simulator:app", args.simulator_port, env))
        services.append(serve("sentinelbridge.app:app", args.port, env))
        while True:
            for service in services:
                code = service.poll()
                if code is not None:
                    return code if code >= 0 else 128 - code
            time.sleep(0.2)
    except KeyboardInterrupt:
        return 0
    finally:
        for service in services:
            if service.poll() is None:
                service.terminate()
        for service in services:
            try:
                service.wait(timeout=5)
            except subprocess.TimeoutExpired:
                service.kill()
                service.wait()


if __name__ == "__main__":
    sys.exit(main())
