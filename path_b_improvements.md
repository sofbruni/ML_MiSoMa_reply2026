# Multi-Agent Data Quality System - Architecture Improvements (Path B)

## Project Context
This is a **LUISS × Reply 2026** university project building a multi-agent system for NoiPA (Italian Public Administration payroll platform) to automatically validate and clean CSV datasets from heterogeneous sources.

**Current Status**: The system works but has architectural inefficiencies where the top supervisor uses an LLM to make routing decisions even though the order is always fixed (1→2→3→4→5).

**Goal**: Transform this into a truly intelligent agentic system with adaptive routing, iteration capabilities, and active remediation.

---

## Core Improvements to Implement

### A. Intelligent Conditional Routing (PRIORITY 1)

**Problem**: Top supervisor currently uses LLM to decide routing but always returns the same sequence. This wastes tokens and adds latency with no benefit.

**Solution**: Make the supervisor actually smart by analyzing dataset characteristics and skipping unnecessary teams.

#### Implementation Steps:

1. **Enhance dataset profiling** (`data_quality/profiling.py` - NEW FILE):
   ```python
   def create_dataset_profile(csv_path: str) -> dict:
       """
       Create comprehensive dataset profile for routing decisions.
       Returns profile with:
       - overall_completeness: float (0-1)
       - numeric_columns: list[str]
       - categorical_columns: list[str]
       - date_columns: list[str]
       - total_columns: int
       - total_rows: int
       - estimated_issues: dict
       """
   ```

2. **Update supervisor logic** (`data_quality/graph.py`):
   - Replace current fixed-sequence supervisor with intelligent routing
   - Add decision logic based on profile:
     - **Always run Schema Team first** (foundational)
     - **Skip Completeness Team** if `overall_completeness > 0.95`
     - **Skip Anomaly Team** if no numeric/categorical columns exist
     - **Skip Consistency Team** if dataset has <3 columns or <10 rows (too simple)
   
   ```python
   def smart_supervisor_node(state: DataQualityState) -> dict:
       """
       Intelligent routing based on dataset characteristics and progress.
       
       Decision tree:
       1. Schema → always first
       2. Completeness → only if <95% complete in initial scan
       3. Consistency → only if meaningful cross-validation possible
       4. Anomaly → only if numeric/categorical columns exist
       5. Remediation → always last
       """
       profile = state["dataset_profile"]
       
       # Track which teams have run
       completed = state.get("completed_teams", set())
       
       # Decision logic
       if "schema" not in completed:
           return {"next": "schema_team"}
       
       # Check if completeness is needed
       if profile["overall_completeness"] < 0.95 and "completeness" not in completed:
           return {"next": "completeness_team"}
       
       # Check if consistency checks make sense
       if (len(profile["columns"]) >= 3 and 
           state.get("total_rows", 0) >= 10 and 
           "consistency" not in completed):
           return {"next": "consistency_team"}
       
       # Check if anomaly detection is applicable
       if (len(profile["numeric_columns"]) > 0 or 
           len(profile["categorical_columns"]) > 0) and "anomaly" not in completed:
           return {"next": "anomaly_team"}
       
       return {"next": "remediation_team"}
   ```

3. **Update state tracking** (`data_quality/state.py`):
   ```python
   class DataQualityState(MessagesState):
       # ... existing fields ...
       completed_teams: set[str] = field(default_factory=set)  # NEW
       iteration_count: int = 0  # NEW
       rows_removed: int = 0  # NEW
   ```

**Expected Benefit**: 
- Saves ~30-40% of LLM calls on clean datasets
- Reduces pipeline runtime by 20-30% on average
- Actually demonstrates "intelligent" routing for the project presentation

---

### B. Iteration Capability (PRIORITY 2)

**Problem**: Teams run once in sequence. If consistency fixes create new completeness issues (e.g., removing duplicates reveals sparse columns), those aren't caught.

**Solution**: Add conditional iteration with max 2 loops to catch cascading issues.

#### Implementation Steps:

1. **Add iteration check function** (`data_quality/graph.py`):
   ```python
   def should_iterate(state: DataQualityState) -> str:
       """
       Decide if we need another pass after certain teams.
       
       Triggers for iteration:
       - Consistency team removed >10% of rows → re-run completeness
       - Schema fixes changed >5 column types → re-run consistency
       
       Max 2 iterations to prevent infinite loops.
       """
       if state.get("iteration_count", 0) >= 2:
           return "remediation_team"  # Force exit
       
       last_team = state.get("last_completed_team")
       
       # After consistency, check if we removed many rows
       if last_team == "consistency":
           rows_removed = state.get("rows_removed_last_team", 0)
           total_rows = state["dataset_profile"]["total_rows"]
           
           if rows_removed > total_rows * 0.1:  # >10% rows removed
               state["iteration_count"] = state.get("iteration_count", 0) + 1
               return "completeness_team"  # Re-check completeness
       
       # After schema, check if many types changed
       if last_team == "schema":
           types_changed = state.get("types_changed_count", 0)
           if types_changed > 5:
               state["iteration_count"] = state.get("iteration_count", 0) + 1
               return "consistency_team"  # Re-check consistency
       
       return "continue"  # No iteration needed
   ```

2. **Update team nodes to track changes**:
   - Schema team: count how many columns had type changes
   - Consistency team: track rows removed by deduplication
   - Store these in state for iteration decision

3. **Modify graph structure** (`data_quality/graph.py`):
   ```python
   # Add conditional edge after key teams
   graph.add_conditional_edges(
       "consistency_team",
       should_iterate,
       {
           "completeness_team": "completeness_team",
           "continue": "top_supervisor"
       }
   )
   ```

**Expected Benefit**:
- Catches 15-20% more data quality issues in real-world messy datasets
- Demonstrates true "agentic" behavior with adaptive iteration
- Strong talking point for project presentation

---

### C. Active Remediation with Confidence Scoring (PRIORITY 3)

**Problem**: Remediation team only suggests fixes but doesn't apply them. It creates v4 as a copy of v3, which is wasteful.

**Solution**: Make remediation team apply high-confidence fixes automatically while logging risky ones.

#### Implementation Steps:

1. **Add confidence scoring to suggestions** (`data_quality/tools/remediation_tools.py`):
   ```python
   def generate_correction_suggestions(findings_json: str) -> dict:
       """
       Enhanced version that includes confidence scores.
       
       Each suggestion now has:
       {
           "field": str,
           "issue": str,
           "action": str,
           "confidence": float (0.0-1.0),
           "risk": "low"|"medium"|"high",
           "auto_apply": bool
       }
       
       Confidence rules:
       - 0.95+: Type coercion, format standardization, whitespace trimming
       - 0.80-0.94: Duplicate removal, obvious outlier fixes
       - 0.50-0.79: Imputation, column dropping
       - <0.50: Complex cross-column fixes, rare value handling
       """
   ```

2. **Create active fix function** (`data_quality/tools/remediation_tools.py`):
   ```python
   def apply_remediation_fixes(
       input_path: str, 
       output_path: str, 
       suggestions: list[dict]
   ) -> dict:
       """
       Apply fixes with confidence >= 0.90 automatically.
       Log fixes with confidence < 0.90 for human review.
       
       Returns:
       {
           "applied_fixes": list[str],
           "skipped_fixes": list[str],  # Low confidence
           "fixes_applied_count": int,
           "manual_review_required": bool
       }
       """
       df = pd.read_csv(input_path)
       applied = []
       skipped = []
       
       for suggestion in suggestions:
           if suggestion["confidence"] >= 0.90 and suggestion["risk"] == "low":
               # Apply safe fixes
               df = apply_single_fix(df, suggestion)
               applied.append(suggestion["action"])
           else:
               # Log for review
               skipped.append(f"{suggestion['action']} (confidence: {suggestion['confidence']:.2f})")
       
       df.to_csv(output_path, index=False)
       
       return {
           "applied_fixes": applied,
           "skipped_fixes": skipped,
           "fixes_applied_count": len(applied),
           "manual_review_required": len(skipped) > 0
       }
   ```

3. **Update remediation team** (`data_quality/teams/remediation_team.py`):
   - After generating suggestions, actually apply high-confidence ones
   - Create v4 with those fixes applied (not just a copy of v3)
   - Include applied vs skipped summary in final report

**Expected Benefit**:
- Completes the "automatic fix" promise from project requirements
- Creates a genuinely more polished final dataset
- Shows AI making confident decisions vs uncertain ones

---

### D. Parallel Execution (PRIORITY 4 - Optional Enhancement)

**Problem**: Schema and Completeness checks are independent but run sequentially.

**Solution**: Use LangGraph's parallel execution to run independent teams simultaneously.

#### Implementation Steps:

1. **Identify independent teams**:
   - Schema + Completeness can run in parallel (both only read original CSV)
   - Consistency + Anomaly CANNOT (anomaly needs consistency fixes applied first)

2. **Update graph structure** (`data_quality/graph.py`):
   ```python
   # Replace sequential edges with parallel execution
   from langgraph.graph import END
   
   # After initial profiling, fan out to parallel execution
   graph.add_edge("start", ["schema_team", "completeness_team"])  # Parallel
   
   # Merge results before consistency
   graph.add_edge(["schema_team", "completeness_team"], "merge_node")
   ```

3. **Create merge node**:
   ```python
   def merge_parallel_results(state: DataQualityState) -> dict:
       """
       Combine findings from parallel teams.
       Merge schema fixes (v1) with completeness fixes (v2_parallel).
       Create combined v2 that has both sets of fixes.
       """
   ```

**Expected Benefit**:
- ~25% faster execution time
- Impressive demo of advanced LangGraph features
- Optional - only implement if time permits

---

### E. Enhanced Scoring with Improvement Roadmap (PRIORITY 3)

**Problem**: Current scoring is one-shot. No guidance on "what to fix first to improve score."

**Solution**: Add prioritized improvement roadmap to remediation output.

#### Implementation Steps:

1. **Enhance reliability scorer** (`data_quality/tools/remediation_tools.py`):
   ```python
   def calculate_reliability_score(findings_json: str) -> dict:
       """
       Enhanced to include:
       - current_score: float
       - current_grade: str
       - deduction_breakdown: dict
       - improvement_roadmap: list[dict]  # NEW
       - expected_score_after_fixes: float  # NEW
       """
       
       # ... existing scoring logic ...
       
       # NEW: Generate improvement roadmap
       roadmap = generate_improvement_roadmap(findings, current_score)
       
       return {
           "reliability_score": current_score,
           "grade": grade,
           "deductions": deductions,
           "improvement_roadmap": roadmap,
           "expected_score_after_fixes": calculate_post_fix_score(findings)
       }
   ```

2. **Create roadmap generator**:
   ```python
   def generate_improvement_roadmap(findings: dict, current_score: float) -> list[dict]:
       """
       Prioritize fixes by impact on score.
       
       Returns:
       [
           {
               "priority": 1,
               "category": "Consistency",
               "action": "Remove 50 duplicate rows",
               "score_impact": +8.5,
               "difficulty": "easy",
               "auto_applicable": True
           },
           ...
       ]
       """
       roadmap = []
       
       # Calculate impact of each fix category
       for category, issues in findings.items():
           impact = estimate_score_impact(category, issues)
           roadmap.append({
               "priority": len(roadmap) + 1,
               "category": category,
               "action": summarize_fixes(issues),
               "score_impact": impact,
               "difficulty": estimate_difficulty(issues),
               "auto_applicable": can_auto_fix(issues)
           })
       
       # Sort by impact (descending)
       roadmap.sort(key=lambda x: x["score_impact"], reverse=True)
       
       return roadmap
   ```

**Expected Benefit**:
- Much better final report for stakeholder presentation
- Shows "before/after" improvement potential
- Aligns with project goal of "correction suggestions"

---

## Implementation Order

### Phase 1 (Week 1): Core Intelligence
1. Implement **A: Intelligent Routing** 
2. Update state tracking and profiling
3. Test with multiple datasets

### Phase 2 (Week 2): Iteration & Active Fixes  
4. Implement **B: Iteration Capability**
5. Implement **C: Active Remediation**
6. Test iteration triggers with edge cases

### Phase 3 (Week 2-3): Polish
7. Implement **E: Enhanced Scoring**
8. Optionally implement **D: Parallel Execution** if time permits
9. Update reports and documentation

---

## Testing Strategy

### Test Datasets Required:

1. **Clean dataset** (>95% complete, no issues)
   - Should skip most teams, fast execution
   - Demonstrates smart routing

2. **Messy dataset** (duplicates, type issues, sparse columns)
   - Should trigger iteration
   - Shows active remediation

3. **Edge case dataset** (2 columns, 5 rows)
   - Should skip consistency and anomaly teams
   - Tests conditional logic

4. **NoiPA-style dataset** (Italian tax/payroll data)
   - Matches project context
   - Real-world validation

### Validation Criteria:

- [ ] Smart routing: clean datasets skip ≥2 teams
- [ ] Iteration: removing >10% rows triggers completeness re-check
- [ ] Active remediation: v4 has more fixes than v3
- [ ] Scoring: roadmap correctly prioritizes high-impact fixes
- [ ] Performance: ≥20% faster on average vs current system

---

## Output Requirements (Per Project Guidelines)

### Required Deliverables:

1. **GitHub Repository** with:
   - Updated code with all improvements
   - Clear README explaining architecture
   - Requirements.txt updated if new dependencies added

2. **PowerPoint Presentation** (suggest Streamlit demo too):
   - Slide 1: Problem statement (NoiPA data quality)
   - Slide 2: Multi-agent architecture overview
   - Slide 3: **Smart routing demo** (before/after comparison)
   - Slide 4: **Iteration capability** (show cascading issue caught)
   - Slide 5: Results with reliability scores
   - Slide 6: Improvement roadmap visualization

3. **Streamlit Interface** (suggested):
   ```python
   # app.py
   import streamlit as st
   
   st.title("NoiPA Data Quality - Multi-Agent System")
   
   uploaded_file = st.file_uploader("Upload CSV")
   
   if uploaded_file:
       # Run pipeline
       results = run_pipeline(uploaded_file)
       
       # Show routing decisions made
       st.subheader("Intelligent Routing")
       st.write(f"Teams executed: {results['teams_run']}")
       st.write(f"Teams skipped: {results['teams_skipped']}")
       
       # Show iteration
       if results['iteration_count'] > 0:
           st.info(f"System ran {results['iteration_count']} iterations")
       
       # Show score and roadmap
       st.metric("Reliability Score", results['score'], results['grade'])
       st.table(results['improvement_roadmap'])
   ```

---

## Key Files to Modify

```
data_quality/
├── profiling.py          # NEW - Enhanced dataset profiling
├── graph.py              # MODIFY - Smart supervisor + iteration logic
├── state.py              # MODIFY - Add iteration tracking fields
├── teams/
│   └── remediation_team.py  # MODIFY - Active fix application
└── tools/
    └── remediation_tools.py  # MODIFY - Confidence scoring + roadmap
```

---

## Success Metrics for Presentation

1. **Efficiency**: "Our smart routing saves 30% of LLM calls on clean data"
2. **Thoroughness**: "Iteration catches 15% more issues than single-pass systems"  
3. **Automation**: "85% of fixes applied automatically with >90% confidence"
4. **Intelligence**: "System adapts routing based on dataset characteristics"

---

## Notes for Claude Code

- Keep existing team structure (don't break 5-team architecture)
- Maintain backward compatibility with existing CSV outputs
- Use Gemini Flash Lite (already configured) - don't change LLM
- Follow existing code style and patterns
- Add comprehensive docstrings to new functions
- Update README.md with new capabilities

**Priority**: Focus on A, B, C first. D and E are enhancements if time permits.

The goal is to transform this from "multi-step pipeline with unnecessary supervisor" into "intelligent multi-agent system with adaptive behavior" - which is what the project brief actually asks for.
