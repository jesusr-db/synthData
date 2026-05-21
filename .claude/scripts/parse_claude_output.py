import re


_BLOCK_RE = re.compile(
    r"=== FILE: (?P<path>[^\s]+) ===[ \t]*\r?\n(?P<content>.*?)=== END FILE ===",
    re.DOTALL,
)


def parse_claude_output(text: str) -> dict[str, str]:
    return {
        m.group("path"): m.group("content").strip()
        for m in _BLOCK_RE.finditer(text)
    }
