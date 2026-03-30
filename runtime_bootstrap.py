"""
Shared runtime/bootstrap helpers for CLI and API entrypoints.
"""

from graph import create_graph
from runtime import create_embedding_client, create_llm_client


def create_runtime_clients(llm_client=None, embedding_client=None):
    """Resolve default runtime clients while allowing overrides."""
    resolved_llm_client = llm_client or create_llm_client()
    resolved_embedding_client = embedding_client or create_embedding_client()
    return resolved_llm_client, resolved_embedding_client


def create_runtime_graph(
    llm_client=None,
    embedding_client=None,
    *,
    db_connection=None,
    enable_embedding_in_graph: bool = True,
):
    """Create a graph with shared runtime client bootstrap."""
    resolved_llm_client, resolved_embedding_client = create_runtime_clients(
        llm_client=llm_client,
        embedding_client=embedding_client,
    )

    graph_embedding_client = resolved_embedding_client if enable_embedding_in_graph else None
    graph = create_graph(
        llm_client=resolved_llm_client,
        embedding_client=graph_embedding_client,
        db_connection=db_connection,
    )

    return graph, resolved_llm_client, resolved_embedding_client
