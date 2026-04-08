from langgraph.graph import MessagesState


class DataQualityState(MessagesState):
    """Top-level shared state for the full pipeline."""
    original_dataset_path: str
    working_dataset_path: str   # updated after each team applies fixes
    dataset_profile: dict       # populated by profiler node; read-only after that
    next: str
    completed_teams: list[str]
    skipped_teams: list[str]
    last_completed_team: str
    iteration_count: int
    rows_removed: int
    rows_removed_last_team: int
    types_changed_count: int
    supervisor_decisions: list[dict]


class TeamState(MessagesState):
    """State used inside each team subgraph."""
    next: str
