# Work Estimation Details and Deliverable Plans Report

Project: Smart Waste Management System (Group F)

Version: 1.0 (Draft for review)

Date: 2026-05-06

## Table of Contents

1. Introduction
2. Estimation Methodology
3. Project Scope Baseline
4. Sprint-wise Work Estimations (5 Sprints)
5. Deliverable Plan
6. Evidence Register
7. Risks and Estimation Assumptions
8. Conclusion

## Revision History

| Name | Date | Reason for Changes | Version |
|---|---|---|---|
| Group F | 2026-05-06 | Initial report created from current repository evidence | 1.0 |

## 1. Introduction

This report documents the work estimation approach and deliverable plan for the Smart Waste Management System across Edge (F1), Data Intelligence (F2), Application Services (F3), and Platform/DevOps (F4).

The structure of this report is adapted from the sample report content provided by the team, while the planning logic aligns with lecture themes in:

- L03 - Agile Software Development
- L08 - Software Testing
- L10 - Project Management
- L12 - DevOps and Code Management

This is an evidence-backed plan. All estimates and deliverables are linked to concrete artifacts already present in the workspace.

## 2. Estimation Methodology

### 2.1 Chosen Estimation Techniques

The team used a hybrid method:

1. Hours-based estimation (primary)
- Tasks are estimated in hours by implementation complexity and integration overhead.
- Used because the repository already tracks technical scope at task level (service READMEs, sprint checklists, integration tests).

2. Expert judgment (team discussion based)
- Estimates are calibrated based on component ownership and existing sprint/task boards.
- F1/F2/F4 artifacts show explicit owner-level milestones and week windows.

3. Risk buffer
- A 15% contingency buffer is applied at sprint level for integration and infrastructure instability.

### 2.2 Contributors to Estimation

- Product/Requirements owners: service and architecture documents maintained in the docs repositories.
- Scrum and sprint leads: explicit sprint checklists and roadmaps in platform and edge planning documents.
- Developers: module-level deliverables in component READMEs and test suites.
- QA/Integration contributors: system integration and verification reports.

### 2.3 Tools and Sources Used for Estimation

- Git repositories and module READMEs
- Docker Compose orchestration and startup scripts
- Integration test suites and verification reports
- Sprint checklist artifacts and roadmap phase plans

## 3. Project Scope Baseline

The current baseline indicates a multi-layer system with the following major workstreams:

1. Edge ingestion and telemetry bridging (F1)
2. Real-time stream processing and route optimization (F2)
3. Workflow orchestration and ML retraining pipelines (F2/F3)
4. Platform hardening, security, observability, and delivery pipelines (F4)
5. End-to-end integration, testing, and reporting (all groups)

Estimated effort baseline (planning envelope):

| Workstream | Estimated Hours |
|---|---:|
| F1 Edge telemetry + bridge + OTA readiness | 200 |
| F2 real-time data layer + ML + route optimization | 260 |
| F3 application integration and workflow coupling | 160 |
| F4 platform, security, observability, CI/CD | 260 |
| End-to-end testing, documentation, demo readiness | 120 |
| **Total (without contingency)** | **1000** |
| Contingency (15%) | 150 |
| **Total (with contingency)** | **1150** |

## 4. Sprint-wise Work Estimations (5 Sprints)

Sprint model used in this plan: 5 sprints, each approximately 2 weeks.

### 4.1 Sprint 1 (Weeks 1-2)

Focus: Foundation, architecture alignment, and environment bootstrap.

| Task | Estimated Hours | Responsible Group(s) |
|---|---:|---|
| Finalize architecture and service contracts | 28 | F2, F3, F4 |
| Bootstrap Docker Compose stack and shared env config | 30 | F2 |
| Initialize core data stores and schemas | 26 | F2, F4 |
| Create Kafka topic baseline and startup scripts | 22 | F2, F4 |
| Set initial CI/lint/test pipelines | 24 | F1, F4 |
| **Sprint 1 Total** | **130** | |

Planned deliverables:
- Initial architecture baseline
- Working local stack startup
- Initial topic and schema baseline

### 4.2 Sprint 2 (Weeks 3-4)

Focus: Core data flow and edge-to-data integration.

| Task | Estimated Hours | Responsible Group(s) |
|---|---:|---|
| Edge simulator/gateway/broker integration milestones | 46 | F1 |
| Flink telemetry processing + scoring | 40 | F2 |
| Route optimizer pipeline and persistence | 34 | F2 |
| ML service core API and fallback behavior | 30 | F2 |
| Cross-team integration smoke tests | 20 | F1, F2, F3 |
| **Sprint 2 Total** | **170** | |

Planned deliverables:
- Telemetry to Kafka to processing pipeline
- Route optimization output to Kafka/PostgreSQL
- Basic predictive API endpoints

### 4.3 Sprint 3 (Weeks 5-6)

Focus: Orchestration, retraining workflows, and reliability.

| Task | Estimated Hours | Responsible Group(s) |
|---|---:|---|
| Airflow DAG pipeline completion (train/promote/publish) | 48 | F2 |
| Spark analytics and model refresh integration | 32 | F2 |
| Platform security hardening (Vault/Istio/OPA tracks) | 44 | F4 |
| Monitoring/logging/tracing setup and dashboards | 34 | F4 |
| Data quality and workflow validation runs | 22 | F2, F4 |
| **Sprint 3 Total** | **180** | |

Planned deliverables:
- Operational Airflow DAG tasks
- Model lifecycle loop through MLflow and service reload
- Core security and observability baseline

### 4.4 Sprint 4 (Weeks 7-8)

Focus: System integration, performance, and hardening.

| Task | Estimated Hours | Responsible Group(s) |
|---|---:|---|
| End-to-end integration testing across layers | 40 | All |
| Load and resilience tests (k6/chaos scenarios) | 36 | F4 |
| Platform deployment hardening and runbooks | 30 | F4 |
| Application workflow timeline and reporting integration | 28 | F3 |
| Defect fixing and regression stabilization | 34 | All |
| **Sprint 4 Total** | **168** | |

Planned deliverables:
- Stable integrated environment
- Verified service communication and data flows
- Initial production hardening artifacts

### 4.5 Sprint 5 (Weeks 9-10)

Focus: Final verification, documentation, and delivery readiness.

| Task | Estimated Hours | Responsible Group(s) |
|---|---:|---|
| Full verification pass and evidence packaging | 34 | F2, F3, F4 |
| Report writing and final technical documentation | 36 | All |
| Demo script, dry runs, and final fixes | 30 | All |
| Release checklist, handoff notes, and acceptance prep | 24 | All |
| **Sprint 5 Total** | **124** | |

Planned deliverables:
- Final test and verification reports
- Final architecture and integration documentation
- Demo-ready and handoff-ready project package

## 5. Deliverable Plan

### 5.1 Deliverable Timeline

| Sprint | Key Deliverables | Description | Responsible Group(s) |
|---|---|---|---|
| Sprint 1 | Foundation baseline (architecture, environment, schemas, topics) | Core technical foundation and development environment readiness | F2, F4 |
| Sprint 2 | Core pipeline (edge to data to optimizer), ML API baseline | First complete value path from telemetry to optimization outputs | F1, F2 |
| Sprint 3 | Airflow+Spark+MLflow loop and platform hardening baseline | Operational orchestration and improved platform reliability | F2, F4 |
| Sprint 4 | Integrated test pass and hardened deployment artifacts | System-level stability, performance and fault tolerance validation | All |
| Sprint 5 | Final reports, demo package, acceptance handoff | Delivery-ready documentation and validated final build | All |

### 5.2 Deliverable Approval Process

Each deliverable is accepted only when all criteria below are satisfied:

1. Technical completion
- Feature or component implemented according to service specification.

2. Evidence completion
- Linked artifact exists (report, test output, config, or implementation file).

3. Verification completion
- Integration and/or system tests pass without unresolved blockers.

4. Review completion
- Sprint review walkthrough by responsible subgroup plus cross-group confirmation where dependencies exist.

## 6. Evidence Register

The following evidence was used to build this estimation and deliverable plan.

| Evidence ID | Artifact | Evidence Summary |
|---|---|---|
| E1 | Data Analysis/README.md | Confirms F2 ownership split (Flink, route optimizer, ML, Airflow, DB) and architecture flow. |
| E2 | Data Analysis/docker-compose.yml | Confirms multi-service orchestration, topic initialization, and integration dependencies. |
| E3 | Data Analysis/ARCHITECTURE_README.md | Confirms completion status, throughput targets, daily retraining, and architecture compliance. |
| E4 | Data Analysis/airflow/README.md | Confirms Airflow/Spark deliverables and batch orchestration responsibilities. |
| E5 | Data Analysis/route-optimizer/README.md | Confirms route optimization IO contracts and deliverables. |
| E6 | Data Analysis/tests/test_integration_pipeline.py | Confirms integration test coverage for MLflow, ML service, and Airflow DAG tasks. |
| E7 | Data Analysis/system_integration_test.py | Confirms system-level phased test framework and infrastructure checks. |
| E8 | Data Analysis/SYSTEM_TEST_REPORT.md | Confirms integrated architecture state, topic registry, service endpoint readiness, and data flow. |
| E9 | Data Analysis/VERIFICATION_REPORT.md | Confirms code/test validation and integration completion evidence. |
| E10 | Smart-Waste-Management-System-Platform/f4-task-list.md | Provides explicit sprint checklist structure for Weeks 1-8 and platform deliverables. |
| E11 | Smart-Waste-Management-System-Platform/DOKS_ROADMAP.md | Provides phased timeline (foundation, security, validation/handoff) and owner allocation. |
| E12 | Edge/Smart-Waste-Management-System-Edge/TEAM_TASKS.md | Provides 2-week edge sprint timeline, owner milestones, and cross-team integration checkpoints. |

## 7. Risks and Estimation Assumptions

### 7.1 Assumptions

- Team capacity remains stable through the 5-sprint window.
- Shared infrastructure and dependencies (Kafka, DBs, orchestration) remain available for integration windows.
- Cross-team contract changes are versioned and communicated early.

### 7.2 Primary Risks

1. Integration risk
- Multiple groups depend on shared topics/schemas and orchestration triggers.

2. Environment risk
- Container/network setup drift can delay integration and test pipelines.

3. Scope expansion risk
- Security/observability and blockchain tracks can grow rapidly without strict cutoff rules.

4. Testing bottleneck risk
- End-to-end tests may fail due to environment rather than code defects, affecting delivery confidence.

### 7.3 Mitigation Strategy

- Keep weekly integration checkpoints and smoke tests.
- Use explicit acceptance criteria per sprint deliverable.
- Preserve contingency buffer and cut non-critical stretch tasks first.

## 8. Conclusion

This report provides a practical and evidence-backed estimation and deliverable plan for the Smart Waste Management System.

The plan uses 5 sprints and aligns with existing repository evidence, current integration maturity, and agile project management practices. By grounding estimates in actual artifacts (implementation files, tests, reports, and sprint checklists), the team can justify workload, track progress objectively, and manage delivery risk with clearer accountability.
