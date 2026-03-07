"""Multi-LLM code review pipeline.

Usage:
    python _code_review.py <prompt_file> [--tag TAG] [--only codex,minimax,kimi]

Examples:
    python _code_review.py _p0_review_prompt.txt --tag p0
    python _code_review.py _p1_review_prompt.txt --tag p1 --only kimi,minimax
"""
import os, json, httpx, time, sys, re, subprocess, argparse

sys.stdout.reconfigure(encoding='utf-8')

BASE = os.path.dirname(os.path.abspath(__file__))


def call_codex(prompt_text):
    """Call Codex (GPT-5.4) via streaming Responses API."""
    url = "https://codex.funai.vip/openai/v1/responses"
    auth_path = os.path.expanduser("~/.codex/auth.json")
    with open(auth_path) as f:
        key = json.load(f).get("OPENAI_API_KEY", "")
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {key}",
        "User-Agent": "OpenAI-Codex-CLI/0.111.0",
    }
    payload = {"model": "gpt-5.4", "input": [{"role": "user", "content": prompt_text}], "stream": True}
    result_bytes = bytearray()
    with httpx.Client(verify=False, timeout=300) as client:
        with client.stream("POST", url, json=payload, headers=headers) as resp:
            if resp.status_code != 200:
                return f"HTTP {resp.status_code}: {resp.read()[:300]}"
            for raw in resp.iter_raw():
                result_bytes.extend(raw)
    raw_text = result_bytes.decode("utf-8", errors="replace")
    result = ""
    for line in raw_text.split("\n"):
        line = line.strip()
        if line.startswith("data: ") and line != "data: [DONE]":
            try:
                data = json.loads(line[6:])
                if "delta" in data.get("type", "") and "delta" in data:
                    result += data["delta"]
            except Exception:
                pass
    return result


def call_minimax(prompt_text):
    """Call MiniMax-M1 via Chat Completions API."""
    url = "https://api.minimax.chat/v1/chat/completions"
    key = "sk-cp-iFmrhTIs8nrF2qfo-umbbUBdYjZCpqF7pOaSxOWTYBznhLbu4O6zJubaYJAMlzkWdMXQ39ly8BWlG-ffOgxJF9yOVVXDyEialsePyDRhR7ToYZuZ9e5mPUY"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {key}"}
    payload = {"model": "MiniMax-M1", "messages": [{"role": "user", "content": prompt_text}], "temperature": 0.3}
    with httpx.Client(verify=False, timeout=300) as client:
        resp = client.post(url, json=payload, headers=headers)
        if resp.status_code != 200:
            return f"HTTP {resp.status_code}: {resp.text[:300]}"
        return resp.json()["choices"][0]["message"]["content"]


def call_kimi(prompt_text):
    """Call Kimi CLI in quiet mode (plain text output)."""
    kimi_exe = os.path.expanduser("~/.local/bin/kimi.exe")
    if not os.path.exists(kimi_exe):
        kimi_exe = r"C:\Users\poped\.local\bin\kimi.exe"
    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    proc = subprocess.run(
        [kimi_exe, "--quiet", "--input-format", "text", "--work-dir", os.path.expanduser("~")],
        input=prompt_text.encode("utf-8"),
        capture_output=True,
        timeout=300,
        env=env,
    )
    raw = proc.stdout.decode("utf-8", errors="replace")
    if not raw and proc.stderr:
        raw = proc.stderr.decode("utf-8", errors="replace")
    result = raw.strip()
    if len(result) < 50:
        return f"Kimi output too short ({len(result)} chars): {result[:500]}"
    return result


REVIEWERS = {
    "codex": ("Codex_GPT54", call_codex),
    "minimax": ("MiniMax_M1", call_minimax),
    "kimi": ("Kimi", call_kimi),
}


def main():
    parser = argparse.ArgumentParser(description="Multi-LLM code review")
    parser.add_argument("prompt_file", help="Path to the review prompt file")
    parser.add_argument("--tag", default="review", help="Tag for output files (e.g. p0, p1)")
    parser.add_argument("--only", default=None, help="Comma-separated list of reviewers (codex,minimax,kimi)")
    args = parser.parse_args()

    prompt_path = os.path.join(BASE, args.prompt_file) if not os.path.isabs(args.prompt_file) else args.prompt_file
    with open(prompt_path, "r", encoding="utf-8") as f:
        prompt = f.read()
    print(f"Prompt: {len(prompt)} chars from {os.path.basename(prompt_path)}")

    selected = args.only.split(",") if args.only else list(REVIEWERS.keys())

    for key in selected:
        if key not in REVIEWERS:
            print(f"Unknown reviewer: {key}. Available: {', '.join(REVIEWERS.keys())}")
            continue
        name, fn = REVIEWERS[key]
        outfile = f"{args.tag}_review_{key}.md"
        print(f"\n{'='*50}\nCalling {name}...")
        start = time.time()
        try:
            result = fn(prompt)
            elapsed = time.time() - start
            if result and len(result) > 200:
                out_path = os.path.join(BASE, outfile)
                with open(out_path, "w", encoding="utf-8") as f:
                    f.write(f"# {name} - {args.tag.upper()} Code Review\n\n{result}\n")
                print(f"  OK: {len(result)} chars, {elapsed:.1f}s -> {outfile}")
            else:
                print(f"  SHORT/EMPTY: {len(result) if result else 0} chars: {(result or '')[:200]}")
        except Exception as e:
            print(f"  ERROR: {type(e).__name__}: {e}")

    print("\nDone!")


if __name__ == "__main__":
    main()
