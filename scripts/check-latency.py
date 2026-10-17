import os
import statistics
import time

from braincore_memory import BrainCore


def main() -> None:
    client = BrainCore(
        base_url=os.getenv("BRAINCORE_URL", "http://127.0.0.1:8787"),
        api_key=os.environ["BRAINCORE_API_KEY"],
        timeout=10,
    )
    scope = {"run_id": "latency-gate"}
    try:
        accepted = client.remember("Latency gate evidence-backed memory.", scope)
        client.wait_for_operation(str(accepted["operation_id"]), timeout=30)
        for _ in range(5):
            client.search("latency gate", scope)
        samples: list[float] = []
        for _ in range(30):
            started = time.perf_counter()
            response = client.search("latency gate", scope)
            samples.append((time.perf_counter() - started) * 1000)
            if not response["results"]:
                raise RuntimeError("latency gate search returned no result")
        p95 = statistics.quantiles(samples, n=100, method="inclusive")[94]
        if p95 > 50:
            raise RuntimeError(f"local HTTP search p95 {p95:.2f}ms exceeds 50ms gate")
        print(f"Local authenticated search p95: {p95:.2f}ms across 30 measured requests.")
    finally:
        client.close()


if __name__ == "__main__":
    main()
