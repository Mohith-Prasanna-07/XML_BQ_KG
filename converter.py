"""
converter.py — Assemble the LLM prompt from a subgraph JSON file and
optionally call the Anthropic API to generate BigQuery SQL or PySpark.

Usage:
    # Dry-run (BigQuery, default)
    python converter.py --subgraph subgraph.json --dry-run --prompt-output pormpt.txt

    # Dry-run (PySpark, single mapping)
    python converter.py --subgraph subgraph.json --target pyspark --dry-run --prompt-output pormpt.txt

    # Dry-run (PySpark, entire workflow — JSON from kg_extractor --workflow)
    python converter.py --subgraph workflow.json --target pyspark-workflow --dry-run --prompt-output pormpt.txt

    # Live BigQuery conversion
    python converter.py --subgraph subgraph.json --api-key sk-ant-... --output result.sql

    # Live PySpark single-mapping conversion
    python converter.py --subgraph subgraph.json --target pyspark --api-key sk-ant-... --output result.py

    # Live PySpark workflow conversion (one or two workflows combined)
    python converter.py --subgraph workflow.json --target pyspark-workflow --api-key sk-ant-... --output pipeline.py

    # API key can also be set via the ANTHROPIC_API_KEY environment variable
"""

import argparse
import json
import os
import sys
from pathlib import Path

from prompts import SQL_SYSTEM_PROMPT, PYSPARK_SYSTEM_PROMPT, PYSPARK_WORKFLOW_SYSTEM_PROMPT
from kg_extractor import CONTEXT_LAYERS

TARGET_PROMPTS = {
    "bq":               SQL_SYSTEM_PROMPT,
    "pyspark":          PYSPARK_SYSTEM_PROMPT,
    "pyspark-workflow": PYSPARK_WORKFLOW_SYSTEM_PROMPT,
}

TARGET_EXTENSIONS = {
    "bq":               ".sql",
    "pyspark":          ".py",
    "pyspark-workflow": ".py",
}

DEFAULT_MODEL      = "claude-opus-4-8"
DEFAULT_MAX_TOKENS = 16000

# Maps converter --target to the CONTEXT_LAYERS key in kg_extractor
_CONTEXT_LAYER_KEY = {
    "bq":               "bq",
    "pyspark":          "pyspark",
    "pyspark-workflow": "pyspark",
}


def load_subgraph(path: str) -> dict:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def assemble_user_message(payload: dict) -> str:
    return json.dumps(payload, indent=2, ensure_ascii=False)


def dry_run(payload: dict, system_prompt: str, prompt_output: str = None) -> None:
    user_msg = assemble_user_message(payload)
    separator = "=" * 72
    lines = [
        separator,
        "SYSTEM PROMPT",
        separator,
        system_prompt,
        "",
        separator,
        "USER MESSAGE  (subgraph JSON)",
        separator,
        user_msg,
        "",
        separator,
        f"Approximate character count — system: {len(system_prompt):,}  "
        f"user: {len(user_msg):,}",
        separator,
    ]
    output = "\n".join(lines)

    if prompt_output:
        Path(prompt_output).write_text(output, encoding="utf-8")
        print(f"Prompt written to: {prompt_output}")
    else:
        print(output)


def call_api(
    payload: dict,
    system_prompt: str,
    api_key: str,
    model: str,
    max_tokens: int,
    output_path: str,
    target: str,
) -> None:
    try:
        import anthropic
    except ImportError:
        print("ERROR: anthropic package not installed. Run: pip install anthropic", file=sys.stderr)
        sys.exit(1)

    if target == "pyspark-workflow":
        wf_names = payload.get("workflow", {}).get("workflow_names", ["output"])
        stem = "_".join(wf_names)
    else:
        stem = payload.get("subgraph", {}).get("mapping_name", "output")
    if not output_path:
        output_path = f"{stem}{TARGET_EXTENSIONS[target]}"

    client = anthropic.Anthropic(api_key=api_key)
    user_message = assemble_user_message(payload)

    print(f"Calling {model} for {stem!r} (target: {target}) …", file=sys.stderr)

    with client.messages.stream(
        model=model,
        max_tokens=max_tokens,
        thinking={"type": "adaptive"},
        system=system_prompt,
        messages=[{"role": "user", "content": user_message}],
    ) as stream:
        message = stream.get_final_message()

    # Extract text block (skip thinking blocks)
    result_text = next(
        (block.text for block in message.content if hasattr(block, "text")),
        None,
    )
    if result_text is None:
        print("ERROR: No text content in API response.", file=sys.stderr)
        sys.exit(1)

    Path(output_path).write_text(result_text, encoding="utf-8")
    print(f"Output written to: {output_path}", file=sys.stderr)
    print(f"Usage — input tokens: {message.usage.input_tokens:,}  "
          f"output tokens: {message.usage.output_tokens:,}", file=sys.stderr)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Convert an Informatica mapping subgraph JSON to BigQuery SQL or PySpark via Claude."
    )
    ap.add_argument("--subgraph",   required=True,             help="Path to the subgraph JSON file (from kg_extractor.py).")
    ap.add_argument("--target",     default="bq",              choices=["bq", "pyspark", "pyspark-workflow"],
                    help="Conversion target: 'bq' (BigQuery SQL, default), 'pyspark' (single mapping), "
                         "or 'pyspark-workflow' (one or more full workflows → one PySpark file).")
    ap.add_argument("--dry-run",    action="store_true",       help="Print the assembled prompt without calling the API.")
    ap.add_argument("--api-key",    default=None,              help="Anthropic API key (or set ANTHROPIC_API_KEY env var).")
    ap.add_argument("--model",      default=DEFAULT_MODEL,     help=f"Claude model ID (default: {DEFAULT_MODEL}).")
    ap.add_argument("--max-tokens", default=DEFAULT_MAX_TOKENS, type=int,
                    help=f"Max output tokens (default: {DEFAULT_MAX_TOKENS}).")
    ap.add_argument("--output",        default=None,
                    help="Output file path (default: <mapping_name>.sql for bq, <mapping_name>.py for pyspark).")
    ap.add_argument("--prompt-output", default=None,
                    help="Save the assembled dry-run prompt to this file instead of printing it.")
    args = ap.parse_args()

    if not Path(args.subgraph).exists():
        print(f"ERROR: File not found: {args.subgraph}", file=sys.stderr)
        sys.exit(1)

    payload = load_subgraph(args.subgraph)
    system_prompt = TARGET_PROMPTS[args.target]

    # Replace the context_layer with the target-appropriate maps so the LLM
    # always receives PySpark types/functions for pyspark targets and BQ types
    # for the bq target, regardless of what was baked into the JSON at extraction time.
    payload["context_layer"] = CONTEXT_LAYERS[_CONTEXT_LAYER_KEY[args.target]]

    if args.dry_run:
        dry_run(payload, system_prompt, args.prompt_output)
        return

    api_key = args.api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print(
            "ERROR: Anthropic API key required. "
            "Pass --api-key or set the ANTHROPIC_API_KEY environment variable.",
            file=sys.stderr,
        )
        sys.exit(1)

    call_api(payload, system_prompt, api_key, args.model, args.max_tokens, args.output, args.target)


if __name__ == "__main__":
    main()
