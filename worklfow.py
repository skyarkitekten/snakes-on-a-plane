"""
Data Modeling Workflow
======================
Orchestrates 4 deep agents via LangGraph to transform FRD + data files
into Snowflake DDL and dbt models.

Install:
    uv sync
"""

import base64
import io
import os
from pathlib import Path
from typing import TypedDict

import fitz  # PyMuPDF
import pandas as pd
import docx
from langchain.chat_models import init_chat_model
from langgraph.graph import END, StateGraph
from deepagents import create_deep_agent

LARGE_FILE_THRESHOLD = 500 * 1024 * 1024  # 500 MB


# ── Helpers ──────────────────────────────────────────────────────────────────

def _count_lines(path: str) -> int:
    """Fast newline counter that reads in 1 MB chunks."""
    count = 0
    with open(path, "rb") as f:
        while chunk := f.read(1 << 20):
            count += chunk.count(b"\n")
    return count


def _describe_images(images: list[bytes]) -> str:
    """Send extracted images to Claude vision and return text descriptions."""
    if not images:
        return ""
    model = init_chat_model("anthropic:claude-sonnet-4-20250514")
    descriptions = []
    for i, img_bytes in enumerate(images, 1):
        b64 = base64.b64encode(img_bytes).decode()
        result = model.invoke([{
            "role": "user",
            "content": [
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:image/png;base64,{b64}"},
                },
                {
                    "type": "text",
                    "text": (
                        "Describe this image in detail for a data architect. "
                        "Focus on any diagrams, tables, charts, schemas, or "
                        "textual content visible in the image."
                    ),
                },
            ],
        }])
        descriptions.append(f"[IMAGE {i}]: {result.content}")
    return "\n\n".join(descriptions)


# ── File Parsing ────────────────────────────────────────────────────────────

def parse_docx(path: str) -> str:
    doc = docx.Document(path)
    text = "\n".join(p.text for p in doc.paragraphs if p.text.strip())

    # Extract embedded images
    images = []
    for rel in doc.part.rels.values():
        if "image" in rel.reltype:
            images.append(rel.target_part.blob)

    if images:
        text += "\n\n## Embedded Images\n\n" + _describe_images(images)

    return text


def parse_pdf(path: str) -> str:
    doc = fitz.open(path)
    text_parts = []
    images = []
    for page in doc:
        text_parts.append(page.get_text())
        for img in page.get_images(full=True):
            xref = img[0]
            pix = fitz.Pixmap(doc, xref)
            if pix.n > 4:  # CMYK or other, convert to RGB
                pix = fitz.Pixmap(fitz.csRGB, pix)
            images.append(pix.tobytes("png"))
    doc.close()

    result = "\n".join(text_parts)
    if images:
        result += "\n\n## Embedded Images\n\n" + _describe_images(images)
    return result


def _parse_large_csv(path: str, sep: str) -> str:
    """Schema + sample for CSV/TSV files over the large-file threshold."""
    file_size_mb = os.path.getsize(path) / (1024 * 1024)
    row_count = _count_lines(path) - 1  # subtract header

    df_sample = pd.read_csv(path, sep=sep, nrows=50)
    dtypes = df_sample.dtypes.to_frame("dtype").reset_index()
    dtypes.columns = ["column", "dtype"]

    return (
        f"**Large file detected** — {file_size_mb:,.1f} MB, ~{row_count:,} rows\n\n"
        f"Columns ({len(df_sample.columns)}): {list(df_sample.columns)}\n\n"
        f"### Column Types\n{dtypes.to_markdown(index=False)}\n\n"
        f"### Sample (first 50 rows)\n{df_sample.to_markdown(index=False)}\n\n"
        f"### Descriptive Stats\n{df_sample.describe(include='all').to_markdown()}"
    )


def parse_file(path: str) -> str:
    """Route to correct parser based on extension."""
    ext = Path(path).suffix.lower()
    file_size = os.path.getsize(path)

    if ext == ".docx":
        return parse_docx(path)
    elif ext == ".pdf":
        return parse_pdf(path)
    elif ext in (".csv", ".tsv"):
        sep = "\t" if ext == ".tsv" else ","
        if file_size > LARGE_FILE_THRESHOLD:
            return _parse_large_csv(path, sep)
        df = pd.read_csv(path, sep=sep, nrows=200)
        return f"Columns: {list(df.columns)}\n\nSample (first 10 rows):\n{df.head(10).to_markdown()}"
    elif ext in (".xlsx", ".xls"):
        df = pd.read_excel(path, nrows=200)
        return f"Columns: {list(df.columns)}\n\nSample (first 10 rows):\n{df.head(10).to_markdown()}"
    elif ext == ".json":
        return Path(path).read_text(encoding="utf-8")[:8000]
    else:
        return Path(path).read_text(encoding="utf-8")[:8000]


# ── Workflow State ──────────────────────────────────────────────────────────

class WorkflowState(TypedDict):
    frd_content: str
    data_file_content: str
    user_prompt: str
    # Agent outputs
    analysis_output: str
    conceptual_model: str
    logical_model: str
    snowflake_ddl: str
    dbt_models: str
    final_output: str


# ── Agent Factory ───────────────────────────────────────────────────────────

def make_agent(system_prompt: str):
    model = init_chat_model("anthropic:claude-sonnet-4-20250514")
    return create_deep_agent(model=model, system_prompt=system_prompt)


def last_message(result: dict) -> str:
    return result["messages"][-1].content


# ── Agent 1: Analysis ───────────────────────────────────────────────────────

def agent1_analyze(state: WorkflowState) -> WorkflowState:
    agent = make_agent(
        "You are a senior business analyst and data architect. "
        "Your job is to analyze functional requirements documents and data files "
        "to extract business entities, relationships, attributes, data types, "
        "business rules, constraints, and domain context. "
        "Be thorough and precise. Output structured markdown."
    )
    result = agent.invoke({"messages": [{
        "role": "user",
        "content": (
            f"## User Directive\n{state['user_prompt']}\n\n"
            f"## Functional Requirements Document\n{state['frd_content']}\n\n"
            f"## Data File\n{state['data_file_content']}\n\n"
            "Produce a structured analysis covering:\n"
            "1. Business domain summary\n"
            "2. Identified entities and their descriptions\n"
            "3. Key attributes per entity with inferred data types\n"
            "4. Relationships between entities (cardinality)\n"
            "5. Business rules and constraints\n"
            "6. Data quality observations from the data file\n"
            "7. Open questions / ambiguities"
        )
    }]})
    state["analysis_output"] = last_message(result)
    return state


# ── Agent 2: Conceptual Data Model ─────────────────────────────────────────

def agent2_conceptual(state: WorkflowState) -> WorkflowState:
    agent = make_agent(
        "You are a data architect specializing in conceptual data modeling. "
        "Transform business analysis into clean, technology-agnostic conceptual models. "
        "Focus on WHAT the business needs, not HOW it will be implemented. "
        "Use standard ER notation in text/markdown form."
    )
    result = agent.invoke({"messages": [{
        "role": "user",
        "content": (
            f"## Business Analysis\n{state['analysis_output']}\n\n"
            "Produce a conceptual data model containing:\n"
            "1. Entity catalog (name, description, business definition)\n"
            "2. Entity-Relationship diagram in text notation (e.g. Crow's Foot)\n"
            "3. Relationship definitions (name, type, cardinality, description)\n"
            "4. Business key candidates per entity\n"
            "5. Subject areas / domain groupings\n"
            "Do NOT include physical attributes, data types, or SQL. "
            "This is business-level, technology-agnostic."
        )
    }]})
    state["conceptual_model"] = last_message(result)
    return state


# ── Agent 3: Logical Data Model ─────────────────────────────────────────────

def agent3_logical(state: WorkflowState) -> WorkflowState:
    agent = make_agent(
        "You are a data modeler specializing in logical data models for cloud analytics. "
        "Transform conceptual models into normalized logical models suitable for "
        "Snowflake data warehousing. Apply Kimball dimensional modeling patterns where "
        "appropriate (facts, dimensions, slowly changing dimensions). "
        "Output structured markdown with precise attribute definitions."
    )
    result = agent.invoke({"messages": [{
        "role": "user",
        "content": (
            f"## Conceptual Data Model\n{state['conceptual_model']}\n\n"
            f"## Original Analysis (for attribute detail)\n{state['analysis_output']}\n\n"
            "Produce a logical data model for Snowflake containing:\n"
            "1. Table catalog (table name, type: fact/dimension/bridge/reference)\n"
            "2. Per table: all columns with name, data type, nullable, PK/FK, description\n"
            "3. Surrogate key strategy (e.g. SEQUENCE or AUTOINCREMENT)\n"
            "4. Foreign key relationships\n"
            "5. SCD type recommendations per dimension (SCD1/SCD2/SCD3)\n"
            "6. Grain definition for each fact table\n"
            "7. Recommended Snowflake schema pattern (star/snowflake/vault)\n"
            "Use Snowflake-compatible data types: VARCHAR, NUMBER, TIMESTAMP_NTZ, "
            "BOOLEAN, VARIANT, ARRAY, OBJECT, DATE, etc."
        )
    }]})
    state["logical_model"] = last_message(result)
    return state


# ── Agent 4: Snowflake Code Artifacts ──────────────────────────────────────

def agent4_snowflake(state: WorkflowState) -> WorkflowState:
    agent = make_agent(
        "You are a Snowflake data engineer and dbt expert. "
        "Generate production-ready Snowflake DDL and dbt models from logical data models. "
        "Follow Snowflake best practices: clustering keys, transient vs permanent tables, "
        "role-based access, column masking policies where sensitive data is present. "
        "Follow dbt best practices: staging/intermediate/mart layer separation, "
        "ref() macro usage, source definitions, generic tests, and documentation blocks."
    )
    result = agent.invoke({"messages": [{
        "role": "user",
        "content": (
            f"## Logical Data Model\n{state['logical_model']}\n\n"
            "Generate TWO artifacts:\n\n"
            "### ARTIFACT 1: Snowflake DDL\n"
            "Produce complete, executable Snowflake SQL including:\n"
            "- Database / schema / warehouse CREATE statements\n"
            "- All CREATE TABLE statements with full column definitions\n"
            "- Primary and foreign key constraints\n"
            "- CLUSTER BY clauses for large fact tables\n"
            "- ROW ACCESS POLICY stubs for sensitive columns\n"
            "- SEQUENCE objects for surrogate keys\n"
            "- Comments on all tables and columns\n"
            "- GRANT statements for roles (analyst, engineer, loader)\n\n"
            "### ARTIFACT 2: dbt Models\n"
            "Produce dbt model files including:\n"
            "- sources.yml (raw source definitions)\n"
            "- staging models (stg_*.sql) — one per source table, light transformation\n"
            "- intermediate models (int_*.sql) — joins and business logic\n"
            "- mart models (fct_*.sql, dim_*.sql) — final consumption layer\n"
            "- schema.yml for each layer with column descriptions and generic tests\n"
            "  (not_null, unique, accepted_values, relationships)\n"
            "- dbt_project.yml snippet\n\n"
            "Clearly delimit each file with a header comment: -- FILE: <filename>"
        )
    }]})
    output = last_message(result)

    # Split DDL and dbt sections
    if "ARTIFACT 2" in output or "dbt Models" in output.lower():
        split_markers = ["### ARTIFACT 2", "## ARTIFACT 2", "# ARTIFACT 2", "**ARTIFACT 2"]
        for marker in split_markers:
            if marker in output:
                parts = output.split(marker, 1)
                state["snowflake_ddl"] = parts[0].strip()
                state["dbt_models"] = marker + parts[1].strip()
                break
        else:
            state["snowflake_ddl"] = output
            state["dbt_models"] = "(dbt section not separately delimited — see DDL output)"
    else:
        state["snowflake_ddl"] = output
        state["dbt_models"] = ""

    return state


# ── Aggregator ──────────────────────────────────────────────────────────────

def aggregate(state: WorkflowState) -> WorkflowState:
    sections = [
        "# Data Modeling Workflow — Final Output",
        "---",
        "## 1. Analysis",
        state["analysis_output"],
        "---",
        "## 2. Conceptual Data Model",
        state["conceptual_model"],
        "---",
        "## 3. Logical Data Model",
        state["logical_model"],
        "---",
        "## 4. Snowflake DDL",
        state["snowflake_ddl"],
    ]
    if state.get("dbt_models"):
        sections += ["---", "## 5. dbt Models", state["dbt_models"]]

    state["final_output"] = "\n\n".join(sections)
    return state


# ── Graph Assembly ──────────────────────────────────────────────────────────

def build_workflow():
    graph = StateGraph(WorkflowState)

    graph.add_node("analyze",    agent1_analyze)
    graph.add_node("conceptual", agent2_conceptual)
    graph.add_node("logical",    agent3_logical)
    graph.add_node("snowflake",  agent4_snowflake)
    graph.add_node("aggregate",  aggregate)

    graph.set_entry_point("analyze")
    graph.add_edge("analyze",    "conceptual")
    graph.add_edge("conceptual", "logical")
    graph.add_edge("logical",    "snowflake")
    graph.add_edge("snowflake",  "aggregate")
    graph.add_edge("aggregate",  END)

    return graph.compile()


# ── Public Entry Point ──────────────────────────────────────────────────────

def run_workflow(
    frd_path: str,
    data_file_path: str,
    user_prompt: str,
    output_path: str = "output.md",
) -> dict:
    """
    Run the full data modeling workflow.

    Args:
        frd_path:       Path to FRD file (.docx or .pdf)
        data_file_path: Path to data file (.csv, .xlsx, .json)
        user_prompt:    User directive (domain context, special instructions)
        output_path:    Where to write the final markdown output

    Returns:
        Final workflow state dict
    """
    frd_content       = parse_file(frd_path)
    data_file_content = parse_file(data_file_path)

    workflow = build_workflow()

    initial_state: WorkflowState = {
        "frd_content":       frd_content,
        "data_file_content": data_file_content,
        "user_prompt":       user_prompt,
        "analysis_output":   "",
        "conceptual_model":  "",
        "logical_model":     "",
        "snowflake_ddl":     "",
        "dbt_models":        "",
        "final_output":      "",
    }

    result = workflow.invoke(initial_state)

    Path(output_path).write_text(result["final_output"], encoding="utf-8")
    print(f"\n✅ Output written to: {output_path}")

    return result


# ── CLI ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Data Model Workflow")
    parser.add_argument("--frd",    required=True, help="Path to FRD (.docx or .pdf)")
    parser.add_argument("--data",   required=True, help="Path to data file (.csv, .xlsx, .json)")
    parser.add_argument("--prompt", required=True, help="User directive / domain context")
    parser.add_argument("--out",    default="output.md", help="Output markdown path")
    args = parser.parse_args()

    run_workflow(
        frd_path=args.frd,
        data_file_path=args.data,
        user_prompt=args.prompt,
        output_path=args.out,
    )