# Architecture: Configuration-Driven Star Schema Generator

## 1. Design Philosophy

This architecture shifts data modeling from manual SQL scripting to a **declarative, component-based pipeline**. Built strictly on pure Python and Jinja2, it completely avoids heavy external data frameworks. The engine acts purely as an orchestrator, generating a mathematically sound, 4-layer Directed Acyclic Graph (DAG) that is optimized for execution engines like Apache Spark.

### The 4-Layer Sequential DAG
To prevent nested subquery hell, the SQL generation flows strictly top-to-bottom:
1. **Shared Components:** Base tables/CTEs computed once and available to all downstream nodes.
2. **Fact Pipeline:** A 3-step assembly line (Pre-Union Staging -> Unpivot/Stack -> Post-Union Bridge).
3. **Dimension Pipeline:** Parameterized logic injected from a catalog, dynamically pruned to output only requested columns.
4. **Adaptive Assembly:** A dynamic `LEFT JOIN` block that mathematically intersects available Fact keys with required Dimension keys to prevent row fan-out.

---

## 2. Project Layout (Modern `src` Structure)

The repository leverages a modern Python layout managed by `uv`, ensuring lightning-fast dependency resolution, isolated environments, and succinct package management.

    star_schema_generator/
    │
    ├── pyproject.toml              # Modern project metadata (replaces setup.py)
    ├── uv.lock                     # Deterministic dependency lockfile
    ├── README.md                   
    │
    ├── config/                     # User Workspace
    │   └── runbook.yml             # The declarative input definition
    │
    ├── templates/                  # SQL & Jinja Template Library
    │   ├── macros/                 
    │   │   └── spark_normalize.sql # Core unpivot/schema-enforcement macro
    │   ├── catalog/                
    │   │   ├── base_users.sql      # Shared CTE logic
    │   │   ├── dim_fraud.sql       # Parameterized dimension logic
    │   │   └── map_sessions.sql    # Key-bridge logic
    │   └── pipelines/              
    │       └── build_master.sql    # The DAG orchestrator template
    │
    ├── src/                        # Isolated Python Package
    │   └── star_gen/               
    │       ├── __init__.py
    │       ├── cli.py              # Command-line interface entry point
    │       ├── parser.py           # YAML ingestion and validation mapping
    │       └── compiler.py         # Jinja2 environment and rendering logic
    │
    └── tests/                      # Pytest suite
        ├── test_parser.py
        └── test_compiler.py

### Python Packaging (`pyproject.toml`)
    [build-system]
    requires = ["hatchling"]
    build-backend = "hatchling.build"

    [project]
    name = "star-schema-generator"
    version = "0.1.0"
    description = "Pure Python orchestration for automated Star Schema generation."
    readme = "README.md"
    requires-python = ">=3.9"
    dependencies = [
        "jinja2>=3.1.0",
        "pyyaml>=6.0.1",
    ]

    [project.scripts]
    stargen = "star_gen.cli:main"

    [tool.pytest.ini_options]
    testpaths = ["tests"]

---

## 3. The Runbook (`config/runbook.yml`)

End-users define the pipeline inputs without writing any SQL. The configuration follows the 4-layer DAG structure.

    # ---------------------------------------------------------
    # LAYER 1: Shared Components
    # ---------------------------------------------------------
    shared_ctes:
      - name: cte_base_users
        catalog_ref: base_users

    # ---------------------------------------------------------
    # LAYER 2: The Fact Pipeline
    # ---------------------------------------------------------
    raw_pks: [user_id, event_date]
    master_pks: [user_id, session_id, event_date, score_type]

    scores:
      tbl_alpha:
        pre_enrich_ref: fix_alpha_currency # Optional Step A (Pre-Union)
        pks: {user_id: u_id, event_date: dt}
        scores: [[math_val, math]]
        
      tbl_beta:
        src_table: raw_beta # Direct read, no pre-enrichment
        pks: {user_id: client_id, event_date: evt_dt}
        scores: [[chem_score, chemistry]]

    post_union_bridge: # Optional Step C (Post-Union)
      catalog_ref: map_sessions
      join_keys: [user_id, event_date]

    # ---------------------------------------------------------
    # LAYER 3: Dimension Enrichments
    # ---------------------------------------------------------
    enrichments:
      - instance_name: fraud_strict
        catalog_ref: dim_fraud
        pks: [user_id, event_date]
        params: 
          threshold: 0.95
        columns: 
          - high_risk_flags

---

## 4. The Orchestrator (`templates/pipelines/build_master.sql`)

The Jinja orchestrator is stripped of nested complexity. It evaluates the YAML and generates a flat, top-to-bottom sequence of CTEs.

    {% import "macros/spark_normalize.sql" as fact %}

    WITH 
    -- ==========================================
    -- LAYER 1: Shared Components
    -- ==========================================
    {% for shared in shared_ctes %}
    {{ shared.name }} AS (
        {% include "catalog/" ~ shared.catalog_ref ~ ".sql" %}
    ),
    {% endfor %}

    -- ==========================================
    -- LAYER 2: The Fact Pipeline
    -- ==========================================
    -- Step A: Pre-Union Staging
    {% for score_name, config in scores.items() %}
        {% if config.pre_enrich_ref %}
        stg_{{ score_name }} AS (
            {% set params = config.params | default({}) %}
            {% include "catalog/" ~ config.pre_enrich_ref ~ ".sql" %}
        ),
        {% endif %}
    {% endfor %}

    -- Step B: Unpivot and Stack
    raw_unified_scores AS (
        {{ fact.spark_normalize(scores, raw_pks) }}
    ),

    -- Step C: Post-Union Bridge
    unified_scores AS (
        {% if post_union_bridge %}
            SELECT 
                fact.*,
                bridge.session_id 
            FROM raw_unified_scores AS fact
            LEFT JOIN (
                {% include "catalog/" ~ post_union_bridge.catalog_ref ~ ".sql" %}
            ) AS bridge
                USING ({{ post_union_bridge.join_keys | join(', ') }})
        {% else %}
            SELECT * FROM raw_unified_scores
        {% endif %}
    ),

    -- ==========================================
    -- LAYER 3: Dimension Enrichments
    -- ==========================================
    {% for dim in enrichments %}
    {{ dim.instance_name }} AS (
        SELECT 
            {{ dim.pks | join(', ') }},
            {{ dim.columns | join(', ') }}
        FROM (
            {% set params = dim.params | default({}) %}
            {% include "catalog/" ~ dim.catalog_ref ~ ".sql" %}
        ) AS inner_query
    ){% if not loop.last %},{% endif %}
    {% endfor %}

    -- ==========================================
    -- LAYER 4: Adaptive Assembly
    -- ==========================================
    SELECT *
    FROM unified_scores
    {% for dim in enrichments %}
    LEFT JOIN {{ dim.instance_name }}
        USING (
            {%- set comma = joiner(", ") -%}
            {%- for key in dim.pks -%}
                {%- if key in master_pks -%}
                    {{ comma() }}{{ key }}
                {%- endif -%}
            {%- endfor -%}
        )
    {% endfor %}

---

## 5. Execution Engine

With `uv` managing the environment, bootstrapping and execution is instant and reproducible without manual virtual environment management.

    # 1. Initialize project and install dependencies
    uv sync

    # 2. Execute the Python CLI command established in pyproject.toml
    uv run stargen --config config/runbook.yml --output dist/final_query.sql
