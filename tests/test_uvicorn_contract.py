import signal
import socket
import subprocess
import sys
import time

import httpx


def test_uvicorn_serves_embedder_and_shuts_down_cleanly() -> None:
    # Reserve the listening socket before the process starts to avoid a port race.
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        port = listener.getsockname()[1]
        process = subprocess.Popen(  # noqa: S603
            [
                sys.executable,
                "-m",
                "uvicorn",
                "braincore_memory.embedder:app",
                "--fd",
                str(listener.fileno()),
                "--log-level",
                "info",
            ],
            pass_fds=(listener.fileno(),),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=1) as client:
                deadline = time.monotonic() + 15
                while True:
                    try:
                        health = client.get("/health")
                        break
                    except httpx.TransportError:
                        assert process.poll() is None, process.communicate()
                        assert time.monotonic() < deadline, "embedder did not start"
                        time.sleep(0.05)
                assert health.status_code == 200
                assert health.json()["dimensions"] == 384
                embedded = client.post("/embed", json={"inputs": ["synthetic evidence"]})
                assert embedded.status_code == 200
                assert len(embedded.json()["embeddings"][0]) == 384
                invalid = client.post("/embed", json={"inputs": []})
                assert invalid.status_code == 422
        finally:
            process.terminate()
            try:
                stdout, stderr = process.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                stdout, stderr = process.communicate(timeout=5)
                raise AssertionError("uvicorn did not shut down") from None
        # Uvicorn completes shutdown and then re-raises the received signal.
        assert process.returncode in (0, -signal.SIGTERM), (stdout, stderr)
        assert b"Application shutdown complete" in stderr, (stdout, stderr)
