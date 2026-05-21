"""Builds the prompt context for doc-update agents.

Targeted mode: reads only the changed source files + current doc contents.
Full-regen mode: reads all mapped source files + queries live Databricks state.
"""

import json
import pathlib
import subprocess
import textwrap


try:
    REPO = pathlib.Path(
        subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip()
    )
except (subprocess.CalledProcessError, FileNotFoundError):
    REPO = pathlib.Path(".")
MAP_PATH = REPO / ".claude" / "doc-update-map.json"
DOCS_DIR = REPO / "docs"


def _read_file_safe(path: pathlib.Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return f"(could not read {path})"


def _all_source_files() -> list[pathlib.Path]:
    """Return all source files referenced in the mapping that exist on disk."""
    with open(MAP_PATH) as f:
        mapping = json.load(f)

    files: set[pathlib.Path] = set()
    for pattern in mapping:
        base = pattern.split("*")[0].rstrip("/")
        base_path = REPO / base
        if base_path.is_file():
            files.add(base_path)
        elif base_path.is_dir():
            suffix = pattern.split("*")[-1].lstrip("/")
            ext = f"*.{suffix.split('.')[-1]}" if "." in suffix else "*"
            files.update(base_path.rglob(ext))

    return sorted(files)


def _databricks_summary() -> str:
    """Run `databricks bundle summary -t dev` and return output, or a fallback note."""
    try:
        out = subprocess.check_output(
            ["databricks", "bundle", "summary", "-t", "dev"],
            text=True, stderr=subprocess.DEVNULL, timeout=30,
        )
        return out[:4000]
    except Exception as e:
        return f"(databricks bundle summary unavailable: {e})"


def _lakebase_schema() -> str:
    """Query INFORMATION_SCHEMA.COLUMNS for zerobus_sdp.* tables via databricks CLI."""
    sql = """
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'zerobus_sdp'
        ORDER BY table_name, ordinal_position
    """
    try:
        out = subprocess.check_output(
            ["databricks", "query", "--warehouse-id", "5067b513037fbf07",
             "--statement", sql, "--output", "JSON"],
            text=True, stderr=subprocess.DEVNULL, timeout=60,
        )
        return out[:6000]
    except Exception as e:
        return f"(live schema unavailable — derive from SQL source files instead. Error: {e})"


def build_targeted(changed_files: list[str], affected_docs: set[str]) -> str:
    """Build a prompt for a targeted (post-commit) doc update."""
    parts = [textwrap.dedent("""\
        You are a documentation maintenance agent for the Claudit observability project.
        Update ONLY the affected documentation files listed below based on the changed source files.

        ## Rules
        1. Regenerate only the sections derived from the changed source files listed here.
        2. Do NOT touch any <!-- NARRATIVE --> ... <!-- /NARRATIVE --> blocks — copy them back verbatim.
        3. Do NOT overwrite existing <!-- TODO: ... --> blocks unless the source data now fills the gap.
        4. Do NOT query any live systems — derive everything from the source file contents provided.
        5. For each affected doc, output the COMPLETE updated file using this exact format:

        === FILE: docs/<name>.md ===
        <complete file content>
        === END FILE ===

        Output ONLY the file blocks — no preamble, no explanation.

        ## Affected docs to update
    """)]

    parts.append(", ".join(sorted(affected_docs)) + "\n\n")

    parts.append("## Changed source file contents\n\n")
    for rel_path in changed_files:
        full_path = REPO / rel_path
        parts.append(f"### {rel_path}\n```\n{_read_file_safe(full_path)}\n```\n\n")

    parts.append("## Current content of affected docs\n\n")
    for doc_name in sorted(affected_docs):
        doc_path = DOCS_DIR / f"{doc_name}.md"
        parts.append(f"### docs/{doc_name}.md\n```markdown\n{_read_file_safe(doc_path)}\n```\n\n")

    return "".join(parts)


def build_full_regen() -> str:
    """Build a prompt for a full-pass regeneration (post-merge-to-main)."""
    parts = [textwrap.dedent("""\
        You are a documentation maintenance agent for the Claudit observability project.
        Regenerate ALL seven documentation files from scratch using the source files and live state provided.

        ## Rules
        1. Generate complete, accurate content for every section.
        2. Where source data is unavailable or unclear, mark the gap:
           <!-- TODO: human narrative needed — <one-line context hint> -->
        3. Do NOT overwrite any <!-- NARRATIVE --> ... <!-- /NARRATIVE --> blocks — copy them back verbatim.
        4. Output ALL seven files using this exact format (no preamble, no explanation):

        === FILE: docs/README.md ===
        <complete file content>
        === END FILE ===

        === FILE: docs/architecture.md ===
        <complete file content>
        === END FILE ===

        (continue for data-model.md, dataflow.md, api.md, quickstart.md, gotchas.md)

        ## File specifications

        **docs/README.md** — one-paragraph project description, table linking to all six docs, "Last regenerated" timestamp.

        **docs/architecture.md** — ASCII system component diagram, deployed resources table (name/type/purpose/status from bundle summary below), design decisions section (why Lakebase, why no PG views, why typed columns).

        **docs/data-model.md** — one table per Delta MV (columns, types, source attribute key), one table per synced PG table using the live schema below, config property → table name mapping.

        **docs/dataflow.md** — end-to-end ASCII flow diagram, pipeline refresh cadence, sync status note (derive from context).

        **docs/api.md** — one section per router with all endpoints: method, path, query params, response shape, cache key, source table. Derived entirely from router/service files below.

        **docs/quickstart.md** — prerequisites (wrap any existing content in NARRATIVE blocks), env vars from config.py with defaults, deploy steps from HANDOFF.md, common commands, known failure modes (wrap existing content in NARRATIVE blocks).

        **docs/gotchas.md** — non-obvious platform behaviors, sharp edges, and workarounds that a developer would only learn by being burned. Organized into sections by subsystem (e.g. DAB / Bundle, AI Gateway, OAuth & OBO tokens, Python packaging, Delta/UC, LangGraph/FMAPI). Each entry: a bold title, 1-2 sentence explanation of the behavior, and the fix or workaround. Include only things that are genuinely surprising — not standard errors covered in quickstart. Preserve any existing <!-- NARRATIVE --> blocks verbatim.

    """)]

    parts.append("## Live Databricks state\n\n### Bundle summary\n```\n")
    parts.append(_databricks_summary())
    parts.append("\n```\n\n### Live PG schema (INFORMATION_SCHEMA.COLUMNS)\n```json\n")
    parts.append(_lakebase_schema())
    parts.append("\n```\n\n")

    parts.append("## All source files\n\n")
    for source_path in _all_source_files():
        rel = source_path.relative_to(REPO)
        parts.append(f"### {rel}\n```\n{_read_file_safe(source_path)}\n```\n\n")

    parts.append("## Current doc contents (for NARRATIVE block preservation)\n\n")
    for doc_name in ["README", "architecture", "data-model", "dataflow", "api", "quickstart", "gotchas"]:
        doc_path = DOCS_DIR / f"{doc_name}.md"
        if doc_path.exists():
            parts.append(f"### docs/{doc_name}.md\n```markdown\n{_read_file_safe(doc_path)}\n```\n\n")

    return "".join(parts)
