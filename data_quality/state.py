from langgraph.graph import MessagesState


class DataQualityState(MessagesState):
    """Top-level shared state for the full pipeline."""
    original_dataset_path: str
    working_dataset_path: str   # updated after each team applies fixes
    dataset_profile: dict       # populated by profiler node; read-only after that
    next: str


class TeamState(MessagesState):
    """State used inside each team subgraph."""
    next: str
