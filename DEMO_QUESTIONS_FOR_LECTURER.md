# Smart Waste Management System - Data Analysis Layer
## Demo Questions for Lecturer Assessment

**Date Created:** May 13, 2026  
**Target Audience:** Lecturer / Evaluator  
**Demo Duration:** 45-60 minutes  
**Group:** F2 (Data Analysis/Intelligence Layer)

---

## Part 1: System Architecture & Design Decisions (10 minutes)

### Fundamental Understanding
1. **System Overview**
   - Can you walk us through the complete data flow from a sensor reading to a route optimization output?
   - What are the 3 main layers in your architecture, and why did you choose this layered approach?

2. **Component Interactions**
   - Why is Kafka placed between Flink and Route Optimizer instead of direct communication?
   - What would happen if PostgreSQL became unavailable during route optimization?

3. **Technology Selection**
   - You chose Google OR-Tools for route optimization. What were the other alternatives considered, and why is OR-Tools superior for waste collection routing?
   - Why use both PostgreSQL and InfluxDB instead of just one database?

### Design Trade-offs
4. **Real-time vs Batch Processing**
   - The system uses both stream processing (Flink) and batch processing (Airflow/Spark). What tasks are suitable for each, and why?
   - How do you prevent duplicate processing when both Flink and Spark might process the same data?

5. **Scalability Considerations**
   - How would your system handle 10x more waste bins? What would be the bottleneck?
   - You've configured 6 Kafka partitions for telemetry. How did you determine this number, and how would you adjust it for scaling?

---

## Part 2: Route Optimizer Deep Dive (10 minutes)

### Algorithm & Implementation
6. **VRP Solver Mechanics**
   - Explain the Vehicle Routing Problem (VRP) and how OR-Tools solves it. What's the difference between VRP and VRPTW (Vehicle Routing Problem with Time Windows)?
   - The system uses "Tabu Search + Guided Local Search" metaheuristics. What does each do, and why combine them?

7. **Constraints & Optimization**
   - Your system defines urgency-based time windows (e.g., urgency 90+ → 0-60 min). Who decides these thresholds, and can they be dynamic?
   - Explain the capacity dimension constraint. How does it handle mixed waste types with different densities?

8. **Fallback Strategy**
   - You mention a "greedy heuristic" fallback if OR-Tools is unavailable. What's the performance difference (quality + speed) compared to optimal routing?
   - Under what conditions would the fallback activate, and how do you monitor for this?

### Data Input & Output
9. **Route Optimizer Input**
   - What's the structure of data flowing from PostgreSQL to the Route Optimizer? Show an example JSON.
   - How often does the optimizer run? Is it triggered by events (e.g., bin reaches 85% full) or scheduled?

10. **Distance Calculation**
    - You use the Haversine formula for distance. Why not use road network distance (from Google Maps API)?
    - The formula includes Earth radius (6,371 km). Why is precision here critical for waste collection routing?

---

## Part 3: Flink Stream Processing (8 minutes)

### Real-time Data Pipeline
11. **Flink Jobs**
    - You have multiple Flink jobs (telemetry, vehicle deviation, zone aggregation). What does each do, and can they run in parallel?
    - How many messages per second can your Flink cluster process? How did you test this?

12. **Windowing & State Management**
    - Explain the windowing strategy for bin telemetry aggregation. Are you using tumbling, sliding, or session windows?
    - Flink stores state. If the Flink job crashes, how do you ensure no data loss and no duplicate processing?

### Data Quality & Reliability
13. **Outlier Detection & Error Handling**
    - How does Flink handle invalid sensor readings (e.g., a bin showing -5% fullness or 250% capacity)?
    - What happens if a sensor sends duplicate messages or messages out-of-order?

14. **Upsert to PostgreSQL**
    - The system upserts bin state to PostgreSQL from Flink. What's your primary key strategy to avoid duplicates?
    - If a sensor reading is 1 hour old when it arrives at Flink, how is it handled?

---

## Part 4: ML Service & Integration (8 minutes)

### Model Deployment & APIs
15. **ML Service Architecture**
    - Your ML Service has 5 prediction endpoints. Can you list them and explain what each predicts?
    - How does MLflow versioning help manage model deployments? Can you roll back to a previous model version without downtime?

16. **Waste Pattern Prediction**
    - What's the training accuracy of your waste prediction model (you mention 94%). What's the test accuracy, and is there overfitting?
    - What features does the model use to predict bin fill rate? How do you handle seasonal variations?

17. **Model Retraining Pipeline**
    - Who triggers retraining—Airflow, or is it manual? How often?
    - If a new model performs worse than the current one, what's the rollback procedure?

---

## Part 5: Data Integration & Kafka (7 minutes)

### Kafka Topic Registry
18. **Topic Design**
    - Show the 9 Kafka topics and explain the data schema for 3 of them.
    - Why do you have both `waste.bin.telemetry` and `waste.bin.processed`? Couldn't Flink write directly to a database?

19. **Consumer Groups & Offsets**
    - You have multiple consumers (Route Optimizer, Spark, possibly others). How do you ensure each consumer can replay messages if needed?
    - What's your retention policy for Kafka topics (how long are messages kept)?

20. **Message Reliability**
    - What's your strategy for exactly-once message delivery vs at-least-once? Which did you choose and why?
    - If a consumer fails while processing a message, what happens to the offset? How is it recovered?

---

## Part 6: Database Schema & Data Integrity (8 minutes)

### PostgreSQL Design
21. **Schema Overview**
    - Show the `bins`, `bin_current_state`, and `route_plans` tables. Why is `bin_current_state` separate from `bins`?
    - What's the primary key and indexing strategy for `bin_current_state`? How many rows do you expect?

22. **Relationships & Constraints**
    - Foreign key constraints: are they enforced in PostgreSQL or at the application level?
    - What happens if a route_plan references a bin that no longer exists?

23. **Upsert Strategy**
    - When Flink upserts to `bin_current_state`, does it use SQL UPSERT (INSERT ... ON CONFLICT), or application-level logic?
    - What columns trigger an update, and which are immutable?

24. **Backup & Recovery**
    - How often is PostgreSQL backed up? Who has backup access?
    - If PostgreSQL becomes corrupted, what's the recovery procedure, and how long does it take?

---

## Part 7: Docker & DevOps (7 minutes)

### Containerization & Orchestration
25. **Docker Compose Setup**
    - Why use Docker Compose for development instead of Kubernetes?
    - What's in the `docker-compose.yml`? List all services and their images.

26. **Container Networking**
    - All services are on the same Docker network. How do they communicate (e.g., how does Route Optimizer reach PostgreSQL)?
    - What's the DNS name for the PostgreSQL service inside containers?

27. **Environment Configuration**
    - The `.env` file has 40+ variables. Show 5 critical ones and explain why they're environment-specific.
    - How do you prevent `.env` secrets (DB passwords, API keys) from being committed to Git?

28. **Startup & Healthchecks**
    - Your docker-compose has healthchecks. What happens if Kafka fails its healthcheck?
    - What's the startup order, and how long does `docker-compose up` take for the full system?

### Deployment & Scaling
29. **Local vs Production**
    - Docker Compose is for local development. How do you deploy to production (you mention Kubernetes migration)?
    - What changes to configurations and images are needed for production?

---

## Part 8: Testing & Verification (7 minutes)

### Test Coverage
30. **System Integration Tests**
    - You have a system integration test suite. What does it test end-to-end?
    - Show a test case: how do you verify that a bin fill sensor reading triggers a route optimization?

31. **Unit vs Integration Testing**
    - Which components have unit tests? Which have integration tests?
    - What's your test coverage percentage? What's untested and why?

32. **Performance & Load Testing**
    - How do you test that the route optimizer can handle 1000 urgent bins? Show the test approach.
    - What metrics do you measure (latency, throughput, memory usage)?

### Validation & Monitoring
33. **System Verification**
    - Your system verification report mentions component checks. What are the 5 most critical checks?
    - How do you verify database integrity after a system restart?

34. **Error Handling & Observability**
    - If a sensor sends invalid data, how do you detect it, log it, and alert operators?
    - Do you have centralized logging? If yes, where are logs stored, and how are they queried?

---

## Part 9: Technical Troubleshooting (8 minutes)

### Real-world Scenarios
35. **Failure Scenarios**
    - **Scenario A:** Flink job crashes mid-processing. How do you ensure the partial bin state isn't saved to PostgreSQL?
    - **Scenario B:** Route Optimizer receives bin data that's 2 hours old. What's the impact on routing quality?
    - **Scenario C:** PostgreSQL rejects an upsert because of a constraint violation. How does Flink respond?

36. **Performance Issues**
    - Route optimization is taking 5 minutes instead of the expected 30 seconds. Walk through your debugging steps.
    - Kafka is lagging. How do you identify if it's a producer, broker, or consumer problem?

37. **Data Consistency**
    - You trust Flink to be the single source of truth for bin state. What if two Flink instances process the same sensor reading?
    - How do you detect and recover from a state where the database and Kafka are out of sync?

---

## Part 10: Evidence & Documentation (5 minutes)

### Artifacts & Proof of Work
38. **Deliverables**
    - Show the commit history. What does each commit represent, and who did it?
    - The WORK_ESTIMATION report estimates 8 hours for the sprint plan but shows 2 minutes actual time. Explain the discrepancy.

39. **Documentation Quality**
    - Is the README sufficient for a new team member to set up and run the system?
    - What documentation is missing?

40. **Knowledge Distribution**
    - Can any team member operate the full system, or is knowledge siloed?
    - If Kalana (who wrote Route Optimizer) leaves, can someone else maintain it?

---

## Bonus Questions (If Time Allows)

### Advanced Scenarios
41. **Geographical Variations**
    - How does your system handle cities with different road networks (urban vs rural)?
    - The Haversine formula works in flat space. How do you account for elevation?

42. **Multi-vehicle Coordination**
    - Can two vehicles collect from the same bin? How does your system prevent this?
    - If vehicle A is delayed, can the route reassign its bins to vehicle B dynamically?

43. **Business Logic**
    - What if a bin is marked as "maintenance required" in the database? Should Route Optimizer exclude it?
    - How do you handle premium customers who demand collection within 1 hour?

44. **Cost Analysis**
    - Can you calculate the cost per route optimization (in terms of CPU/memory)?
    - If you ran the optimizer every minute vs every 5 minutes, what's the cost impact?

45. **Future Enhancements**
    - What's the next feature you'd add to the Data Analysis layer?
    - How would you integrate real-time traffic data from Google Maps API?

---

## Evaluation Rubric for Lecturer

| Category | Excellent (A) | Good (B) | Satisfactory (C) | Needs Improvement (D) |
|----------|---------------|----------|------------------|----------------------|
| **Architecture Understanding** | Can explain all components and their interactions | Can explain most components | Can explain basic flow | Cannot explain system |
| **Technical Depth** | Can answer advanced questions and scenarios | Can answer most questions with detail | Can answer basic questions | Struggles with technical questions |
| **System Design** | Design choices are justified and optimal | Design is reasonable with minor issues | Design works but suboptimal | Design has flaws |
| **Problem Solving** | Can debug complex issues | Can identify issues and propose solutions | Can identify issues | Cannot troubleshoot |
| **Documentation** | Clear, comprehensive, easy to follow | Mostly complete and clear | Adequate but gaps exist | Incomplete or confusing |
| **Team Collaboration** | All members contribute equally | Most members contribute well | Some knowledge silos | High knowledge silos |
| **DevOps & Deployment** | Full CI/CD pipeline with observability | Docker/Kubernetes working well | Basic Docker setup | Manual, ad-hoc deployment |

---

## Notes for Lecturer

1. **Progression:** Questions start simple and escalate in complexity. Stop at the level the group is comfortable with.
2. **Observation:** Watch for:
   - Who answers each question (knowledge distribution)
   - If they read documentation or demonstrate deep understanding
   - How they handle "I don't know" (confidence vs bluffing)
3. **Customization:** Feel free to skip sections or ask follow-ups based on what matters most to your evaluation.
4. **Group Dynamics:** If one person dominates, ask others to explain their components.
5. **Live Demo:** Consider asking them to:
   - Start the full system with `docker-compose up`
   - Trigger a sensor reading and trace it through Kafka → Flink → Route Optimizer → PostgreSQL
   - Show logs and prove data integrity

---

**Created by:** GitHub Copilot  
**Last Updated:** May 13, 2026
