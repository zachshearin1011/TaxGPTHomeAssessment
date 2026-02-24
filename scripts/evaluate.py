#!/usr/bin/env python3
import json
import logging
import sys
import time
from pathlib import Path

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("evaluate")

BASE_URL = "http://localhost:8000"
EVAL_PATH = Path(__file__).resolve().parent.parent / "eval" / "evaluation_dataset.json"
RESULTS_PATH = Path(__file__).resolve().parent.parent / "eval" / "evaluation_results.json"


def main():
    with open(EVAL_PATH) as f:
        dataset = json.load(f)

    client = httpx.Client(base_url=BASE_URL, timeout=120.0)

    try:
        r = client.get("/api/health")
        r.raise_for_status()
    except Exception as e:
        logger.error("Server not reachable at %s: %s", BASE_URL, e)
        logger.info("Start the server first: python manage.py runserver")
        sys.exit(1)

    results = []
    total = len(dataset["questions"])

    for i, q in enumerate(dataset["questions"], 1):
        client.post("/api/chat", json={"message": "reset", "reset": True})

        logger.info("[%d/%d] %s: %s", i, total, q["id"], q["question"][:80])

        start = time.time()
        try:
            resp = client.post("/api/chat", json={"message": q["question"]})
            resp.raise_for_status()
            data = resp.json()
            elapsed = time.time() - start

            result = {
                "id": q["id"],
                "category": q["category"],
                "question": q["question"],
                "answer": data["answer"],
                "sources": data["sources"],
                "query_type": data["query_type"],
                "expected_approach": q["expected_approach"],
                "validation_hints": q["validation_hints"],
                "latency_seconds": round(elapsed, 2),
                "approach_match": data["query_type"] == q["expected_approach"]
                    or q["expected_approach"] in ("hybrid", "graph"),
            }
            results.append(result)
            logger.info("  -> %s (%.1fs) sources=%s", data["query_type"], elapsed, data["sources"])

        except Exception as e:
            logger.error("  -> FAILED: %s", e)
            results.append({
                "id": q["id"],
                "question": q["question"],
                "error": str(e),
            })

    answered = [r for r in results if "answer" in r]
    approach_matches = sum(1 for r in answered if r.get("approach_match", False))
    avg_latency = sum(r.get("latency_seconds", 0) for r in answered) / max(len(answered), 1)

    summary = {
        "total_questions": total,
        "answered": len(answered),
        "errors": total - len(answered),
        "approach_match_rate": f"{approach_matches}/{len(answered)}",
        "avg_latency_seconds": round(avg_latency, 2),
    }

    output = {"summary": summary, "results": results}

    with open(RESULTS_PATH, "w") as f:
        json.dump(output, f, indent=2)

    logger.info("Evaluation complete. Results saved to %s", RESULTS_PATH)
    logger.info("Summary: %s", json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()