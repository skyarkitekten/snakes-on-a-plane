# Snakes On A Plane

Prototype that uses a multi-agent LangGraph workflow to turn FRDs + data files into Snowflake DDL and dbt models. Powered by Claude via the Anthropic API.

## Quick Start

```bash
# 1. Install uv (if you don't have it)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 2. Install dependencies
uv sync

# 3. Set your Anthropic API key
export ANTHROPIC_API_KEY="sk-..."

# 4. Run the workflow
uv run python workflow.py \
  --frd requirements.docx \
  --data customers.csv \
  --prompt "Financial services customer data warehouse, PII-sensitive" \
  --out output.md
```

## Prerequisites

- Python 3.14+
- [uv](https://docs.astral.sh/uv/)
- An [Anthropic API key](https://console.anthropic.com/)

## Supported File Types

| Input | Formats                          |
| ----- | -------------------------------- |
| FRD   | `.docx`, `.pdf`, `.txt`          |
| Data  | `.csv`, `.tsv`, `.xlsx`, `.json` |
