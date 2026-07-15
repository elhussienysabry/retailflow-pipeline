"""
RetailFlow Pipeline — Dynamic Lineage Graph Exporter
=====================================================

Parses dbt's compiled ``manifest.json``, builds a ``networkx.DiGraph``
of model dependencies (Staging → Intermediate → Marts), and renders a
high-resolution visual lineage diagram colour-coded by architectural layer.

Output: ``docs/lineage/current_data_lineage.png``

Usage:
    python scripts/generate_lineage.py
"""

import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

import matplotlib.patheffects as path_effects
import networkx as nx
from matplotlib import pyplot as plt

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MANIFEST_PATH = PROJECT_ROOT / "dbt" / "target" / "manifest.json"
OUTPUT_DIR = PROJECT_ROOT / "docs" / "lineage"
OUTPUT_PATH = OUTPUT_DIR / "current_data_lineage.png"

# ── Layer mapping ───────────────────────────────────────────────────────
# Node names follow the pattern: ``{layer}_{entity}``.
# Colour palette chosen for accessibility (ColorBrewer-inspired).

LAYER_COLORS: Dict[str, str] = {
    "staging": "#2E8B57",  # sea green
    "intermediate": "#4169E1",  # royal blue
    "marts": "#DAA520",  # goldenrod
    "source": "#808080",  # grey (not rendered as model node)
}

LAYER_ORDER: List[str] = ["staging", "intermediate", "marts"]


# ── Helpers ─────────────────────────────────────────────────────────────


def _detect_layer(node_name: str) -> str:
    """Infer the architectural layer from the model name.

    Looks for known prefixes: ``stg_``, ``int_``, ``dim_``, ``fct_``.
    Falls back to ``"marts"`` for anything unexpected.
    """
    lower = node_name.lower()
    if lower.startswith("stg_"):
        return "staging"
    if lower.startswith("int_"):
        return "intermediate"
    if lower.startswith(("dim_", "fct_")):
        return "marts"
    # fallback
    return "marts"


def _short_name(unique_id: str) -> str:
    """Strip the dbt prefix to get the human-readable model name.

    ``"model.retailflow.stg_customers"`` → ``"stg_customers"``
    """
    return unique_id.rsplit(".", 1)[-1]


def _is_model_node(node_data: Dict[str, Any]) -> bool:
    """Return ``True`` if the manifest node describes a dbt model."""
    return node_data.get("resource_type") == "model"


def _is_source_ref(ref: str) -> bool:
    """Return ``True`` if a dependency reference points to a source."""
    return ref.startswith("source.")


# ── Graph construction ──────────────────────────────────────────────────


def build_lineage_graph(manifest: Dict[str, Any]) -> nx.DiGraph:
    """Build a directed graph of model dependencies from the dbt manifest.

    Only ``model.*`` nodes are included; ``source.*`` references are
    skipped (they represent raw tables, not transformed models).

    Returns:
        A ``networkx.DiGraph`` where each node stores:
            - ``layer`` — ``"staging"``, ``"intermediate"``, or ``"marts"``
            - ``label`` — short model name (e.g. ``"stg_customers"``)
    """
    graph = nx.DiGraph()
    nodes: Dict[str, Any] = manifest.get("nodes", {})

    # Phase 1: collect all model nodes.
    for node_id, node_data in nodes.items():
        if not _is_model_node(node_data):
            continue
        name = _short_name(node_id)
        layer = _detect_layer(name)
        graph.add_node(node_id, label=name, layer=layer)

    # Phase 2: add edges for ``ref()`` dependencies only.
    for node_id, node_data in nodes.items():
        if not _is_model_node(node_data):
            continue
        depends = node_data.get("depends_on", {}).get("nodes", [])
        for dep in depends:
            if _is_source_ref(dep):
                continue
            # Only add the edge if the dependency is also a model node
            # (it should always be, but guard against edge cases).
            if dep in graph:
                graph.add_edge(dep, node_id)

    logger.info(
        "Lineage graph built: %d nodes, %d edges",
        graph.number_of_nodes(),
        graph.number_of_edges(),
    )
    return graph


# ── Visualisation ───────────────────────────────────────────────────────


def _layer_positions(
    graph: nx.DiGraph,
) -> Dict[str, Tuple[float, float]]:
    """Compute layered positions for the DAG.

    Each layer (staging / intermediate / marts) occupies a vertical
    column.  Nodes within each layer are spread evenly along the
    vertical axis.  This layout mirrors the left-to-right flow of
    the pipeline architecture diagram.
    """
    layer_nodes: Dict[str, List[str]] = {ln: [] for ln in LAYER_ORDER}
    for node, data in graph.nodes(data=True):
        layer = data.get("layer", "marts")
        if layer in layer_nodes:
            layer_nodes[layer].append(node)

    pos: Dict[str, Tuple[float, float]] = {}
    x_spacing = 3.0
    for i, layer in enumerate(LAYER_ORDER):
        nodes = layer_nodes[layer]
        count = len(nodes)
        if count == 0:
            continue
        x = i * x_spacing
        for j, node in enumerate(nodes):
            y = (count - 1) / 2.0 - j
            pos[node] = (x, y)

    return pos


def render_lineage_graph(graph: nx.DiGraph) -> Path:
    """Render the lineage graph to a high-resolution PNG.

    Args:
        graph: The model dependency graph.

    Returns:
        Path to the saved image.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    pos = _layer_positions(graph)

    # Colours per node.
    node_colors = []
    for _, data in graph.nodes(data=True):
        layer = data.get("layer", "marts")
        node_colors.append(LAYER_COLORS.get(layer, "#808080"))

    # Labels.
    labels = {n: data.get("label", n) for n, data in graph.nodes(data=True)}

    # ── Figure setup ────────────────────────────────────────────────
    fig, ax = plt.subplots(1, 1, figsize=(14, 6), facecolor="#FAFAFA")
    ax.set_facecolor("#FAFAFA")

    # ── Draw edges with arrowheads ──────────────────────────────────
    nx.draw_networkx_edges(
        graph,
        pos,
        ax=ax,
        edge_color="#BBBBBB",
        arrows=True,
        arrowstyle="-|>",
        arrowsize=16,
        min_source_margin=18,
        min_target_margin=18,
        width=1.5,
        alpha=0.7,
    )

    # ── Draw nodes ──────────────────────────────────────────────────
    nx.draw_networkx_nodes(
        graph,
        pos,
        ax=ax,
        node_color=node_colors,
        node_shape="o",
        node_size=2200,
        edgecolors="#333333",
        linewidths=1.5,
        alpha=0.95,
    )

    # ── Draw labels ─────────────────────────────────────────────────
    for node, (x, y) in pos.items():
        label = labels[node]
        ax.text(
            x,
            y,
            label,
            ha="center",
            va="center",
            fontsize=9,
            fontweight="bold",
            color="#FFFFFF",
            path_effects=[
                path_effects.Stroke(linewidth=1.5, foreground="#222222"),
                path_effects.Normal(),
            ],
        )

    # ── Layer annotation boxes ──────────────────────────────────────
    layer_labels = {
        "staging": "Staging",
        "intermediate": "Intermediate",
        "marts": "Marts",
    }
    for i, layer in enumerate(LAYER_ORDER):
        x = i * 3.0
        ax.annotate(
            layer_labels[layer],
            xy=(x, pos[list(pos.keys())[0]][1] if pos else 0),
            xytext=(x, max(y for _, y in pos.values()) + 1.2),
            ha="center",
            fontsize=12,
            fontweight="bold",
            color=LAYER_COLORS[layer],
            annotation_clip=False,
        )

    # ── Style ───────────────────────────────────────────────────────
    ax.set_title(
        "RetailFlow Pipeline — Data Lineage DAG",
        fontsize=16,
        fontweight="bold",
        pad=20,
        color="#333333",
    )
    ax.axis("off")
    plt.tight_layout()

    # ── Save ────────────────────────────────────────────────────────
    fig.savefig(
        OUTPUT_PATH,
        dpi=200,
        bbox_inches="tight",
        facecolor=fig.get_facecolor(),
        edgecolor="none",
    )
    plt.close(fig)

    logger.info("Lineage graph saved to %s", OUTPUT_PATH)
    return OUTPUT_PATH


# ── CLI entry point ─────────────────────────────────────────────────────


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    if not MANIFEST_PATH.exists():
        logger.error(
            "manifest.json not found at %s.  Run `dbt compile` or "
            "`dbt docs generate` first.",
            MANIFEST_PATH,
        )
        return 1

    with open(MANIFEST_PATH, encoding="utf-8") as f:
        manifest = json.load(f)

    graph = build_lineage_graph(manifest)

    if graph.number_of_nodes() == 0:
        logger.warning("No model nodes found in manifest — nothing to render.")
        return 1

    output = render_lineage_graph(graph)
    print(f"\n  Lineage blueprint saved to: {output}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
