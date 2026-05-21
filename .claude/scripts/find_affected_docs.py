import re


def _glob_to_regex(pattern: str) -> re.Pattern:
    """Convert a glob pattern (supporting * and **) to a compiled regex."""
    parts = []
    i = 0
    while i < len(pattern):
        if pattern[i : i + 3] == "**/":
            # **/ matches zero or more directory segments (including none)
            parts.append("(.+/)?")
            i += 3
        elif pattern[i : i + 2] == "**":
            # ** at the end matches anything
            parts.append(".*")
            i += 2
        elif pattern[i] == "*":
            # * matches anything except a path separator
            parts.append("[^/]*")
            i += 1
        elif pattern[i] == ".":
            parts.append(r"\.")
            i += 1
        else:
            parts.append(re.escape(pattern[i]))
            i += 1
    return re.compile(f"^{''.join(parts)}$")


def find_affected_docs(mapping: dict, changed_files: list[str]) -> set[str]:
    affected: set[str] = set()
    compiled = {pattern: _glob_to_regex(pattern) for pattern in mapping}

    for file_path in changed_files:
        for pattern, regex in compiled.items():
            if regex.match(file_path):
                affected.update(mapping[pattern])

    return affected
