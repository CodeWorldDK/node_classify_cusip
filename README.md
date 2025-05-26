Metric Calculation DAG System — Developer Specification
1. Overview
Build and orchestrate a Directed Acyclic Graph (DAG) for metric calculation with layered architecture:

Data Sourcing (Snowflake, AWS S3, Legend Lambdas APIs)

Preprocessing

Metric Calculation (multiple code-based nodes in Python, Scala, Java)

Review

The system:

Uses Dagster as the orchestration engine.

Runs as daily batch jobs with event-driven triggers.

Uses OAuth with a centralized token service for API authentication.

Employs a centralized catalog service for unified metadata and snapshot dataset management.

Enables retries, audit logs, failure handling, and alerting (email, PagerDuty).

Supports multi-language nodes with code-defined inputs and outputs mapped to datasets via catalog service.

Supports unit testing with BDD and snapshot testing, executed in isolated scratch environments.

Uses serverless functions for modular operations, managed separately and invoked via events.

Writes results initially to S3, then persists to Snowflake via a persistor function.

2. Architecture & Components
2.1 Data Sources
Snowflake as main database.

AWS S3 for storing files and snapshots.

Legend Lambdas as API sources.

2.2 Orchestration
Dagster orchestrates DAG runs, node executions, error handling, retries, and halting downstream on failures.

Event-based triggering with external event services.

Configurations via static config files for event-node mappings.

2.3 Metadata & Catalog
Centralized metadata registry in a database, caching metadata locally for efficiency.

Maintains dataset coordinates, versions, schema, environment configs per node.

Locked dataset versions per DAG run to ensure reproducibility.

2.4 Metric Calculation Nodes
Nodes written in Python, Scala, or Java.

Node inputs/outputs are logical dataset names; actual dataset details pulled from catalog.

Each node writes to intermediate or shared tables.

Nodes have associated unit tests (BDD and snapshot).

Nodes store test configs locally but reference snapshots stored centrally.

2.5 Testing Framework
Common testing framework repo handles test orchestration, environment setup, running BDD & snapshot tests, report generation.

Fetches baseline snapshot datasets from Central Catalog dynamically.

Executes tests in isolated scratch schemas with configurable environments.

Supports multi-language test execution.

Snapshot datasets are centralized, versioned, and shared.

2.6 Results Persistence
Node outputs initially stored in S3.

Persister function writes outputs to Snowflake.

Orchestrator tracks success/failure and triggers retries or alerts.

3. Error Handling & Alerting
DAG run audit logs stored separately from metadata.

On node failure or validation rejection, downstream nodes halted.

Alerts via email and PagerDuty integration.

Orchestrator manages retries with configurable policies.

4. Repository Structure
4.1 Common Repos (shared by all metrics)
datasourcing-common

testing-framework-common

central-catalog-service

4.2 Metric-Specific Repos (per metric)
css
Copy
Edit
metric-[metric_name]/
├── preprocessing/
├── metric_calculation/
│   └── node_xyz/
│       ├── src/
│       ├── tests/
│       │   ├── bdd/
│       │   ├── test_config.yaml
│       │   └── run_tests.sh
│       └── README.md
├── review/
└── pipeline-configs/
Common repos are integrated as git submodules or via CI artifact dependencies.

Each metric repo handles DAG definitions, retry policies, and metric-specific logic.

5. Testing Plan
5.1 Unit Testing Types
BDD Tests: Define behavior scenarios, stored per node.

Snapshot Tests: Input and baseline output datasets centrally stored and fetched dynamically via catalog API.

5.2 Test Execution Flow
Node test configs define which snapshot datasets to use by dataset IDs and versions.

Testing Framework queries Central Catalog API to get snapshot metadata and download links.

Framework sets up isolated scratch schema environment.

Runs node transformation code on input snapshots, compares outputs with baseline snapshots.

Reports pass/fail and detailed diffs.

5.3 Test Config Example (node-level test_config.yaml)
yaml
Copy
Edit
node_name: node_xyz
test_environment: dev_scratch_schema
snapshots:
  inputs:
    - dataset_id: pos_data_v1
    - dataset_id: ref_data_v3
  baseline_outputs:
    - dataset_id: node_xyz_expected_output_v2
bdd_tests:
  - feature_file: bdd/behavior_scenario1.feature
  - feature_file: bdd/behavior_scenario2.feature
5.4 Central Catalog Snapshot API
GET /snapshots?node_name=...&test_environment=... returns snapshot metadata and download URLs.

Dataset schema, format, checksum, size included for validation.

6. CI/CD Pipelines
6.1 Common Repos
Build, test, and publish packages/artifacts (e.g., Python wheels, Docker images).

Semantic versioning, manual publish triggers.

6.2 Metric Repos
Use Git submodules to integrate common repos.

Pipeline stages: Init (submodules), Build, Test (using common testing framework), Deploy.

Use GitLab variables for secrets.

Manual or automatic deployment of serverless functions and DAG configs.

7. Sample API Spec for Central Catalog Snapshot Fetch
Request
GET /snapshots?node_name=node_xyz&test_environment=dev_scratch_schema

Response
json
Copy
Edit
{
  "node_name": "node_xyz",
  "test_environment": "dev_scratch_schema",
  "snapshots": {
    "inputs": [
      {
        "dataset_id": "pos_data_v1",
        "schema": {...},
        "storage_location": "s3://bucket/path/pos_data_v1.csv",
        "version": "v1.2",
        "checksum": "sha256:abcdef123456...",
        "format": "csv",
        "size_bytes": 123456
      }
    ],
    "baseline_outputs": [
      {
        "dataset_id": "node_xyz_expected_output_v2",
        "schema": {...},
        "storage_location": "s3://bucket/path/node_xyz_expected_output_v2.csv",
        "version": "v2.0",
        "checksum": "sha256:987654fedcba...",
        "format": "csv",
        "size_bytes": 654321
      }
    ]
  }
}
