"""
Local embedding-based theme classifier — Step 6 of the thematic classification pipeline.

Uses SentenceTransformer (all-MiniLM-L6-v2 by default) to encode statements
and approved themes, then computes cosine similarity to find the best match.
"""
import logging
import threading
from typing import Dict, Any, List, Optional, Tuple

import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

from app.config import settings

logger = logging.getLogger("analytics_service.services.classifier")

# Module-level model cache — loaded once per worker process
_model: Optional[SentenceTransformer] = None
# Real OS-thread lock, not asyncio.Lock — this is called from separate threads
# via asyncio.to_thread (build_theme_embeddings/get_theme_similarities), not
# concurrently within one event loop.
_model_lock = threading.Lock()


def _get_model() -> SentenceTransformer:
    """
    Lazily load the sentence transformer model (cached at module level).
    Thread-safe: double-checked locking, since multiple concurrent activities
    call this from separate OS threads. Without the lock, several threads can
    all see _model is None at once and each construct their own
    SentenceTransformer concurrently — that's not safe (load-tested: it
    corrupts PyTorch's internal state with "NotImplementedError: Cannot copy
    out of meta tensor; no data" under concurrent load). The un-locked fast
    path below keeps the common case (already loaded) lock-free.
    """
    global _model
    if _model is not None:
        return _model
    with _model_lock:
        if _model is None:
            model_name = settings.EMBEDDING_MODEL_NAME
            logger.info(f"Loading SentenceTransformer model '{model_name}'...")
            _model = SentenceTransformer(model_name)
            logger.info(f"SentenceTransformer model '{model_name}' loaded successfully.")
    return _model


def build_theme_embeddings(approved_themes: List[Dict[str, Any]]) -> Dict[str, np.ndarray]:
    """
    Build embeddings for each approved theme, matching the representation used
    by the offline thematic_analysis.py discovery tool:
      - one base vector encoding name + definition + keywords combined
      - one additional vector per '|'-delimited example statement

    Returns: {theme_id_str: np.ndarray of shape (N, embedding_dim)}
    """
    model = _get_model()
    theme_vectors: Dict[str, np.ndarray] = {}

    for theme in approved_themes:
        theme_id = str(theme["id"])
        name = theme.get("name", "") or ""
        definition = theme.get("definitions", "") or theme.get("definition", "") or ""
        keywords = theme.get("keywords", "") or ""
        examples = theme.get("examples", "") or ""

        base_text = f"Theme: {name}. Definition: {definition}. Keywords: {keywords}."
        base_emb = np.array(model.encode(base_text)).reshape(1, -1)

        example_list = [ex.strip() for ex in examples.split("|") if ex.strip()]
        if example_list:
            example_embs = np.array(model.encode(example_list))
            vectors = np.vstack([base_emb, example_embs])
        else:
            vectors = base_emb

        theme_vectors[theme_id] = vectors

    return theme_vectors


def get_theme_similarities(
    statement: str,
    theme_vectors: Dict[str, np.ndarray],
) -> List[Tuple[str, float]]:
    """
    Computes each theme's best (max) cosine similarity to the statement.

    Returns a list of (theme_id, similarity_score) sorted by score descending.
    Empty list if no themes available.
    """
    if not theme_vectors:
        return []

    model = _get_model()
    stmt_emb = model.encode(statement).reshape(1, -1)

    scores: List[Tuple[str, float]] = []
    for theme_id, vectors in theme_vectors.items():
        sims = cosine_similarity(stmt_emb, vectors)[0]
        scores.append((theme_id, float(np.max(sims))))

    scores.sort(key=lambda pair: pair[1], reverse=True)
    return scores


def classify_statement(
    statement: str,
    theme_vectors: Dict[str, np.ndarray],
    theme_id_to_info: Dict[str, Dict[str, Any]],
) -> Tuple[Optional[str], float]:
    """
    Classify a single statement against pre-computed theme embeddings,
    returning only the single best match. See get_theme_similarities()
    for the full ranked list (used by multi-theme callers).

    Returns:
        (best_theme_id, best_similarity_score)
        If no themes available, returns (None, 0.0)
    """
    scores = get_theme_similarities(statement, theme_vectors)
    if not scores:
        return None, 0.0
    return scores[0]
