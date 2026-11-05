"""Expand local composite steps for release script and ordering tests."""

from pathlib import Path
import re
import textwrap

ROOT = Path(__file__).resolve().parents[2]


def workflow_source(name="release"):
    source = (ROOT / f".github/workflows/{name}.yml").read_text()
    parts = re.split(r"(?=^      - name: )", source, flags=re.M)
    for index, step in enumerate(parts):
        action = re.search(r"^        uses: (\./\.github/actions/[^\s]+)$", step, re.M)
        if action:
            definition = (ROOT / action[1] / "action.yml").read_text()
            body = definition.split("  steps:\n", 1)[1]
            parts[index] = textwrap.indent(body, "  ")
    return "".join(parts)
