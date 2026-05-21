#!/usr/bin/env python3
"""PostToolUse hook entry point for living doc updates.

Reads Claude Code PostToolUse JSON from stdin.
Fires only when a Bash git commit or git merge/push-to-main was executed.
Runs claude -p to update affected docs, then stages and commits the changes.
"""

import json
import pathlib
import subprocess
import sys


def _get_bash_command(stdin_data: dict) -> str:
    return stdin_data.get("tool_input", {}).get("command", "")


def _is_git_commit(cmd: str) -> bool:
    import re
    return bool(re.search(r"\bgit\s+commit\b", cmd))


def _is_merge_to_main(cmd: str) -> bool:
    import re
    return bool(
        re.search(r"\bgit\s+merge\b", cmd)
        or re.search(r"\bgit\s+push\b.*\borigin\s+main\b", cmd)
    )


def _run_claude(prompt: str, repo: pathlib.Path) -> str:
    """Run claude -p with the given prompt (via stdin) and return stdout."""
    try:
        result = subprocess.run(
            ["claude", "-p", "-"],
            input=prompt, capture_output=True, text=True, timeout=300,
            cwd=str(repo),
        )
        if result.returncode != 0:
            _log(f"claude -p failed: {result.stderr[:500]}")
            return ""
        return result.stdout
    except subprocess.TimeoutExpired:
        _log("claude -p timed out after 300s")
        return ""
    except FileNotFoundError:
        _log("claude CLI not found — is it on PATH?")
        return ""


def _apply_and_commit(file_updates: dict[str, str], commit_msg: str, repo: pathlib.Path) -> None:
    """Write updated doc files to disk, stage them, and commit."""
    if not file_updates:
        _log("No file updates parsed from claude output")
        return

    for rel_path, content in file_updates.items():
        full_path = repo / rel_path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_text(content + "\n", encoding="utf-8")
        _log(f"Updated {rel_path}")

    staged = subprocess.run(
        ["git", "add"] + list(file_updates.keys()),
        cwd=str(repo), capture_output=True,
    )
    if staged.returncode != 0:
        _log(f"git add failed: {staged.stderr.decode()}")
        return

    diff = subprocess.run(
        ["git", "diff", "--cached", "--quiet"],
        cwd=str(repo),
    )
    if diff.returncode == 0:
        _log("No doc changes to commit")
        return

    committed = subprocess.run(
        ["git", "commit", "-m", commit_msg],
        cwd=str(repo), capture_output=True, text=True,
    )
    if committed.returncode != 0:
        _log(f"git commit failed: {committed.stderr}")
    else:
        _log(f"Committed: {commit_msg}")


def _log(msg: str) -> None:
    print(f"[doc-update] {msg}", file=sys.stderr)


def main() -> None:
    # Resolve repo root inside main() so import doesn't crash if git is unavailable
    try:
        repo = pathlib.Path(
            subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip()
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return  # not in a git repo or git not available

    scripts = repo / ".claude" / "scripts"
    sys.path.insert(0, str(scripts))

    from find_affected_docs import find_affected_docs
    from build_context import build_targeted, build_full_regen
    from parse_claude_output import parse_claude_output

    try:
        data = json.load(sys.stdin)
    except json.JSONDecodeError:
        return  # not a valid hook call — exit silently

    cmd = _get_bash_command(data)

    if _is_merge_to_main(cmd):
        _log("Full regeneration triggered by merge/push to main")
        prompt = build_full_regen()
        output = _run_claude(prompt, repo)
        updates = parse_claude_output(output)
        _apply_and_commit(updates, "docs: full regeneration on merge to main", repo)

    elif _is_git_commit(cmd):
        try:
            changed_raw = subprocess.check_output(
                ["git", "diff", "--name-only", "HEAD~1", "HEAD"],
                text=True, cwd=str(repo), stderr=subprocess.DEVNULL,
            ).strip()
        except subprocess.CalledProcessError:
            return  # first commit or other git error — skip

        if not changed_raw:
            return

        changed_files = changed_raw.splitlines()
        map_path = repo / ".claude" / "doc-update-map.json"
        with open(map_path) as f:
            mapping = json.load(f)

        affected = find_affected_docs(mapping, changed_files)
        # skip if only doc files changed (avoid recursion)
        if all(cf.startswith("docs/") for cf in changed_files):
            return
        if not affected:
            return

        _log(f"Targeted update for: {sorted(affected)} (changed: {changed_files})")
        prompt = build_targeted(changed_files, affected)
        output = _run_claude(prompt, repo)
        updates = parse_claude_output(output)
        affected_paths = {f"docs/{d}.md" for d in affected}
        filtered = {k: v for k, v in updates.items() if k in affected_paths}
        _apply_and_commit(
            filtered,
            f"docs: auto-update {','.join(sorted(affected))} after commit",
            repo,
        )


if __name__ == "__main__":
    main()
