"""
CertSafari scraper — step through quizzes question-by-question.
/api/questions has a separate (more lenient) rate limit from /api/create-quiz.
Strategy: create quiz → drain all questions → create next quiz → repeat.
"""
import requests
import json
import uuid
import time
import sys
import os

BASE = "https://www.certsafari.com"
HEADERS = {
    "Content-Type": "application/json",
    "Origin": BASE,
    "Referer": f"{BASE}/databricks/quiz/data-engineer-professional",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
}
CERT = "data-engineer-professional"
VENDOR = "databricks"
TARGET = 355
OUT_PATH = "mock_exam/certsafari_all_questions.json"
STATE_PATH = "mock_exam/_scrape_state.json"

QUESTION_DELAY = 3
CREATE_DELAY = 60
RATE_429_WAIT = 120

session = requests.Session()
session.headers.update(HEADERS)

def load_state():
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            state = json.load(f)
        return {q["id"]: q for q in state.get("questions", [])}
    return {}

def save_progress(all_questions):
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump({"questions": sorted(all_questions.values(), key=lambda q: q["id"])},
                  f, ensure_ascii=False, indent=2)

def save_final(all_questions):
    result = sorted(all_questions.values(), key=lambda q: q["id"])
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    return result

def post(path, body):
    for attempt in range(5):
        try:
            r = session.post(f"{BASE}{path}", json=body, timeout=30)
            if r.status_code == 429:
                wait = RATE_429_WAIT * (attempt + 1)
                print(f"  [429] {path} — waiting {wait}s (attempt {attempt+1})", flush=True)
                time.sleep(wait)
                continue
            if r.status_code == 200:
                return r.json()
            print(f"  [HTTP {r.status_code}] {path}", flush=True)
            return None
        except Exception as e:
            print(f"  [ERR] {path}: {e}", flush=True)
            time.sleep(10)
    return None

def drain_quiz(quiz_id, first_question, total, all_questions):
    """Step through all questions in a quiz."""
    new_count = 0
    last_domain = None

    if first_question:
        qid = first_question["id"]
        if qid not in all_questions:
            all_questions[qid] = first_question
            new_count += 1
        last_domain = first_question.get("domain")

    for i in range(1, total):
        time.sleep(QUESTION_DELAY)
        resp = post("/api/questions", {
            "quiz_id": quiz_id,
            "domain": last_domain,
            "is_correct": True,
        })
        if not resp:
            print(f"    Failed at q {i+1}/{total}", flush=True)
            break

        data = resp.get("data")
        q = data[0] if isinstance(data, list) and data else data if isinstance(data, dict) and "id" in data else None

        if not q or "id" not in q:
            if resp.get("data") is None or resp.get("data") == []:
                print(f"    Quiz exhausted at q {i+1}/{total}", flush=True)
            break

        if q["id"] not in all_questions:
            all_questions[q["id"]] = q
            new_count += 1
        last_domain = q.get("domain")

        if (i + 1) % 20 == 0:
            save_progress(all_questions)
            print(f"    {i+1}/{total} stepped, +{new_count} new, total={len(all_questions)}", flush=True)

    return new_count

def main():
    all_questions = load_state()
    if all_questions:
        print(f"Resumed: {len(all_questions)} questions", flush=True)

    quiz_num = 0
    stale_quizzes = 0

    while len(all_questions) < TARGET and stale_quizzes < 8:
        quiz_num += 1
        user_id = str(uuid.uuid4())

        print(f"\n--- Quiz #{quiz_num} (have {len(all_questions)}/{TARGET}) ---", flush=True)

        resp = post("/api/create-quiz", {
            "certificate": CERT, "vendor": VENDOR,
            "n_questions": 100, "user_id": user_id,
            "mode": "exam", "domain": None,
        })
        if not resp or "data" not in resp:
            print("  create-quiz failed, will retry after delay", flush=True)
            time.sleep(CREATE_DELAY)
            continue

        quiz = resp["data"]["quiz"]
        quiz_id = quiz["id"]
        total = len(quiz["question_ids"])
        first_q = resp["data"].get("first_question")

        known = sum(1 for qid in quiz["question_ids"] if qid in all_questions)
        print(f"  Quiz {quiz_id}: {total} questions, {known} already known", flush=True)

        new = drain_quiz(quiz_id, first_q, total, all_questions)
        save_progress(all_questions)
        print(f"  +{new} new questions, total={len(all_questions)}/{TARGET}", flush=True)

        if new == 0:
            stale_quizzes += 1
            print(f"  Stale ({stale_quizzes}/8)", flush=True)
        else:
            stale_quizzes = 0

        if len(all_questions) < TARGET:
            print(f"  Waiting {CREATE_DELAY}s before next quiz...", flush=True)
            time.sleep(CREATE_DELAY)

    result = save_final(all_questions)
    print(f"\n=== DONE: {len(result)} questions saved to {OUT_PATH} ===", flush=True)

    if os.path.exists(STATE_PATH):
        os.remove(STATE_PATH)

if __name__ == "__main__":
    main()
