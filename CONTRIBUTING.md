# Contributing to RetailFlow Pipeline

Thank you for your interest in contributing! This guide will help you get started.

## How to Add a New dbt Model

### Step 1: Decide which layer it belongs to

| Layer | When to use |
|-------|-------------|
| `staging/` | Raw → clean rename, cast, basic null handling |
| `intermediate/` | Join staging tables, enrich data |
| `marts/` | Final business-ready tables (dims + facts) |

### Step 2: Create the SQL model file

Create a file in the appropriate `dbt/models/<layer>/` folder.

**Example** — adding `stg_suppliers.sql` in `dbt/models/staging/`:

```sql
-- stg_suppliers.sql
-- Raw suppliers → clean staging model

WITH source AS (
    SELECT * FROM {{ source('raw', 'suppliers') }}
),

cleaned AS (
    SELECT
        supplier_id,
        TRIM(supplier_name) AS supplier_name,
        TRIM(country) AS country,
        is_active
    FROM source
    WHERE supplier_id IS NOT NULL
)

SELECT * FROM cleaned
```

### Step 3: Create the matching `.yml` file

Create `stg_suppliers.yml` in the same folder:

```yaml
version: 2

models:
  - name: stg_suppliers
    description: Clean staging model for supplier data
    columns:
      - name: supplier_id
        description: Primary key for suppliers
        tests:
          - unique
          - not_null
      - name: supplier_name
        description: Supplier company name
        tests:
          - not_null
      - name: country
        description: Country where the supplier is based
      - name: is_active
        description: Whether the supplier is currently active
        tests:
          - accepted_values:
              values: [true, false]
```

### Step 4: Add the source definition (if new source table)

Edit `dbt/models/staging/stg_<your_table>.sql` or add to `sources.yml`.

### Step 5: Run your model

```bash
cd dbt
dbt run --select stg_suppliers
dbt test --select stg_suppliers
```

## Code Style Guidelines

- **SQL**: Use CTEs only (no nested subqueries). Capitalize SQL keywords.
- **Python**: Use type hints on all functions. Add docstrings to every module and function.
- **Comments**: Add a `-- WHY:` comment for any non-obvious SQL logic. Add `# WHY:` for Python.

## Pull Request Process

1. Create a feature branch: `git checkout -b feature/your-feature`
2. Make your changes and run `make test` and `make lint`
3. Push your branch and open a Pull Request
4. Include a clear description of what your change does and why
