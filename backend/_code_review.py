"""Multi-LLM code review pipeline.

Usage:
    # Mode 1: Provide a pre-built prompt file
    python _code_review.py prompt <prompt_file> --tag p1

    # Mode 2: Auto-generate prompt from git diff (recommended)
    python _code_review.py diff --tag p1 [--base HEAD~1] [--summary "描述本阶段变更"]

    # Options
    --only codex,minimax,kimi    Only use specified reviewers
    --dry-run                    Generate prompt only, don't send

Examples:
    python _code_review.py diff --tag p1 --base "4349b1d" --summary "P1: 风险预警+归因增强"
    python _code_review.py prompt _p1_review_prompt.txt --tag p1 --only kimi
"""
import os, json, httpx, time, sys, re, subprocess, argparse

sys.stdout.reconfigure(encoding='utf-8')

BASE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(BASE)

REVIEW_TEMPLATE = """你是FOF投研平台的高级代码评审专家。以下是平台{tag}阶段的代码变更，请做全面评审。

## {tag}变更概述
{summary}

## 评审维度
1. **安全性** - 认证、注入、权限控制、敏感数据处理
2. **数据完整性** - 事务边界、约束、并发安全
3. **性能** - 索引、N+1查询、缓存策略、内存使用
4. **代码质量** - 错误处理、日志、类型安全、异步正确性
5. **架构** - 分层合理性、耦合度、可测试性

## 输出要求
请给出：
- 总体评分(0-100)
- PASS/FAIL判定
- 每个问题包含：严重级别(Critical/Major/Minor)、文件位置(file:line)、问题描述、具体修复建议（含代码）

代码如下：

{code_blocks}"""


def generate_prompt_from_diff(tag, base, summary):
    """Generate review prompt from git diff."""
    # Get changed files
    cmd = ["git", "diff", "--name-only", base, "HEAD"]
    proc = subprocess.run(cmd, capture_output=True, text=True, cwd=REPO_ROOT)
    if proc.returncode != 0:
        # Fallback: use staged + unstaged changes
        cmd = ["git", "diff", "--name-only", "HEAD"]
        proc = subprocess.run(cmd, capture_output=True, text=True, cwd=REPO_ROOT)

    files = [f.strip() for f in proc.stdout.strip().split("\n") if f.strip()]
    # Filter to relevant source files
    files = [f for f in files if f.endswith(('.py', '.ts', '.tsx', '.toml'))
             and not f.startswith(('_', '.'))
             and '__pycache__' not in f]

    if not files:
        print("No changed files found. Check --base parameter.")
        sys.exit(1)

    print(f"Found {len(files)} changed files")

    # Build code blocks
    code_blocks = []
    total_chars = 0
    for f in sorted(files):
        full_path = os.path.join(REPO_ROOT, f)
        if not os.path.exists(full_path):
            continue
        with open(full_path, "r", encoding="utf-8", errors="replace") as fh:
            content = fh.read()
        if len(content) > 50000:
            content = content[:50000] + "\n... (truncated)"
        code_blocks.append(f"=== {f} ===\n{content}")
        total_chars += len(content)

    if not code_blocks:
        print("No readable files found.")
        sys.exit(1)

    prompt = REVIEW_TEMPLATE.format(
        tag=tag.upper(),
        summary=summary or f"{tag} 阶段代码变更",
        code_blocks="\n\n".join(code_blocks),
    )

    print(f"Prompt: {len(prompt)} chars from {len(code_blocks)} files ({total_chars} chars code)")
    return prompt


# ---------------------------------------------------------------------------
# LLM callers
# ---------------------------------------------------------------------------

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
    """Call Kimi CLI in quiet mode (plain text output).

    Key: must set PYTHONUTF8=1 to avoid surrogate encoding errors on Windows.
    """
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


def run_reviews(prompt, tag, selected):
    """Send prompt to selected reviewers and save results."""
    results = {}
    for key in selected:
        if key not in REVIEWERS:
            print(f"Unknown reviewer: {key}. Available: {', '.join(REVIEWERS.keys())}")
            continue
        name, fn = REVIEWERS[key]
        outfile = f"{tag}_review_{key}.md"
        print(f"\n{'='*50}\nCalling {name}...")
        start = time.time()
        try:
            result = fn(prompt)
            elapsed = time.time() - start
            if result and len(result) > 200:
                out_path = os.path.join(BASE, outfile)
                with open(out_path, "w", encoding="utf-8") as f:
                    f.write(f"# {name} - {tag.upper()} Code Review\n\n{result}\n")
                print(f"  OK: {len(result)} chars, {elapsed:.1f}s -> {outfile}")
                results[key] = {"status": "ok", "chars": len(result), "time": elapsed, "file": outfile}
            else:
                print(f"  SHORT/EMPTY: {len(result) if result else 0} chars: {(result or '')[:200]}")
                results[key] = {"status": "short", "chars": len(result) if result else 0}
        except Exception as e:
            print(f"  ERROR: {type(e).__name__}: {e}")
            results[key] = {"status": "error", "error": str(e)}

    # Summary
    print(f"\n{'='*50}\nReview Summary:")
    for key, r in results.items():
        name = REVIEWERS[key][0]
        if r["status"] == "ok":
            print(f"  {name}: {r['chars']} chars, {r['time']:.1f}s -> {r['file']}")
        else:
            print(f"  {name}: {r['status']} - {r.get('error', r.get('chars', ''))}")

    return results


def main():
    parser = argparse.ArgumentParser(description="Multi-LLM code review pipeline")
    subparsers = parser.add_subparsers(dest="mode")

    # Mode 1: prompt file
    p_prompt = subparsers.add_parser("prompt", help="Use a pre-built prompt file")
    p_prompt.add_argument("prompt_file", help="Path to the review prompt file")

    # Mode 2: git diff
    p_diff = subparsers.add_parser("diff", help="Auto-generate prompt from git diff")
    p_diff.add_argument("--base", default="HEAD~1", help="Base commit for diff (default: HEAD~1)")
    p_diff.add_argument("--summary", default=None, help="Summary of changes")

    # Common options
    for p in [p_prompt, p_diff]:
        p.add_argument("--tag", default="review", help="Tag for output files (e.g. p0, p1)")
        p.add_argument("--only", default=None, help="Comma-separated reviewers (codex,minimax,kimi)")
        p.add_argument("--dry-run", action="store_true", help="Generate prompt only, don't send")

    args = parser.parse_args()

    if not args.mode:
        parser.print_help()
        sys.exit(1)

    # Build prompt
    if args.mode == "prompt":
        prompt_path = os.path.join(BASE, args.prompt_file) if not os.path.isabs(args.prompt_file) else args.prompt_file
        with open(prompt_path, "r", encoding="utf-8") as f:
            prompt = f.read()
        print(f"Prompt: {len(prompt)} chars from {os.path.basename(prompt_path)}")
    else:  # diff
        prompt = generate_prompt_from_diff(args.tag, args.base, args.summary)

    # Save prompt for reference
    prompt_save = os.path.join(BASE, f"_{args.tag}_review_prompt.txt")
    with open(prompt_save, "w", encoding="utf-8") as f:
        f.write(prompt)
    print(f"Prompt saved: {prompt_save}")

    if args.dry_run:
        print("Dry run - prompt generated but not sent.")
        return

    # Run reviews
    selected = args.only.split(",") if args.only else list(REVIEWERS.keys())
    run_reviews(prompt, args.tag, selected)
    print("\nDone!")


if __name__ == "__main__":
    main()
