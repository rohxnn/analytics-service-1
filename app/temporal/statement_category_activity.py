import asyncio
import json
import logging
from typing import Any, Dict

from temporalio import activity

from app.config import settings
from app.database.db import db
from app.database.operations import (
    fetch_statements_for_submission,
    insert_analysis_result,
    update_submission_status,
)

from app.services.classifier import load_setfit_model, predict_setfit_batch

logger = logging.getLogger("analytics_service.temporal.statement_category")


# -------------------------------------------------------------------------
# LLM fallback prompt — used when SetFit confidence is below threshold.
# -------------------------------------------------------------------------
LLM_CATEGORIZATION_PROMPT = """You are an expert data annotator and ML data cleaner. Your task is to categorize sentences from community discussions about education and social issues into exactly one of three categories: Challenge, Solution or Action, or Other.

You must be completely consistent and follow these strict guidelines:

Category 1: Challenge
Definition: The sentence describes a problem, obstacle, barrier, hardship, or a lack of resources that prevents a positive outcome (like going to school).
Key Indicators: "cannot", "unable to", "due to lack of", "problem", "difficult", "far away" (without a means to travel).
Example: "She is not going to school because she does not have a bicycle."
Example: "Many girls cannot study because schools are far away."

Category 2: Solution or Action
Definition: The sentence describes a step taken, an action performed, a suggestion given, or a resource provided to solve a problem or improve a situation.
Crucial Rule: If a sentence mentions a problem, but ALSO mentions how it is being solved, overcome, or addressed (e.g., "The school is far, BUT the government gave bicycles"), it MUST be categorized as Solution or Action.
Key Indicators: "decided to", "arranged for", "motivated", "advised", "providing", "can go by".
Example: "If it is far away, you can go to school by bicycle." (Proposing a solution)
Example: "The community decided to get Aadhaar cards made for the children." (Action taken)
Example: "Due to the school being far away, I will arrange for a bicycle." (Action taken to overcome a challenge)

Category 3: Other
Definition: The sentence is a general statement, a fact, greetings, or contextual information that does not clearly articulate a specific barrier nor a specific action/solution.
Example: "The members told the people sitting there that they can go home but listen for 5 minutes."
Example: "Some people are aware about education."

Text: "{text}"

Return ONLY a valid JSON object matching this format (no markdown, no extra text):
{{"category": "Challenge or Solution or Action or Other", "confidence": 0.XX, "justification": "Brief reason"}}"""


def _llm_classify(text: str):
    """Call the LLM to classify a single statement (blocking)."""
    from app.services.llm import openrouter_chat_completion

    prompt = LLM_CATEGORIZATION_PROMPT.replace("{text}", text)
    response_text, usage = openrouter_chat_completion(prompt)

    # Clean markdown wrappers if present
    cleaned = response_text.strip()
    if cleaned.startswith("```"):
        lines = cleaned.splitlines()
        if lines[0].startswith("```json") or lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        cleaned = "\n".join(lines).strip()

    try:
        result = json.loads(cleaned)
    except json.JSONDecodeError as e:
        logger.error("Failed to parse LLM response: %s (raw: %s)", e, response_text[:200])
        result = {"category": "Other", "confidence": 0.0, "justification": "LLM parse error"}

    return result, usage


# -------------------------------------------------------------------------
# Temporal activity definition
# -------------------------------------------------------------------------
@activity.defn
async def statement_category_activity(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Temporal activity: classifies each statement for a submission using a
    SetFit model, falling back to LLM when confidence is below threshold.
    Results are stored in the analysis_results table.
    """
    submission_id = params["submission_id"]
    tenant_code = params["tenant_code"]
    threshold = settings.SETFIT_CONFIDENCE_THRESHOLD

    logger.info(
        "Starting statement categorization for submission=%s tenant=%s (threshold=%.2f)",
        submission_id, tenant_code, threshold,
    )

    # 1. Fetch original statements (skip duplicates with parent_id set)
    async with db.pool.acquire() as conn:
        statements = await fetch_statements_for_submission(conn, submission_id, tenant_code)

    if not statements:
        logger.info("No statements found for submission=%s — skipping.", submission_id)
        return {"status": "skipped", "reason": "no statements found"}

    logger.info("Found %d statements to classify for submission=%s", len(statements), submission_id)

    # 2. Load the SetFit model (blocking — run in thread)
    model = await asyncio.to_thread(
        load_setfit_model,
        settings.SETFIT_MODEL_ID,
        settings.SETFIT_MODEL_VERSION,
    )

    # 3. Batch predict with SetFit (blocking — run in thread)
    texts = [s["raw_statement"] for s in statements]
    predictions, confidence_scores = await asyncio.to_thread(predict_setfit_batch, model, texts)

    # 4. Process each result — LLM fallback if below threshold
    results = []
    for i, stmt in enumerate(statements):
        model_pred = str(predictions[i])
        model_conf = float(confidence_scores[i])

        llm_pred = None
        llm_conf = None
        justification = None

        if model_conf < threshold:
            # LLM fallback
            logger.info(
                "Statement [%s] confidence %.2f < threshold %.2f — calling LLM fallback.",
                stmt["id"], model_conf, threshold,
            )
            try:
                llm_result, usage = await asyncio.to_thread(_llm_classify, stmt["raw_statement"])
                llm_pred = llm_result.get("category")
                llm_conf = llm_result.get("confidence")
                justification = llm_result.get("justification")
            except Exception as e:
                logger.error("LLM fallback failed for statement [%s]: %s", stmt["id"], e)
                llm_pred = "Other"
                llm_conf = 0.0
                justification = f"LLM error: {e}"

        final_category = llm_pred if llm_pred else model_pred

        results.append({
            "statement_id": stmt["id"],
            "statement_type": stmt["statement_type"],
            "model_pred": model_pred,
            "model_conf": model_conf,
            "llm_pred": llm_pred,
            "llm_conf": llm_conf,
            "justification": justification,
            "final_category": final_category,
        })

    # 5. Bulk insert into analysis_results
    async with db.pool.acquire() as conn:
        for r in results:
            await insert_analysis_result(
                conn,
                submission_id=submission_id,
                tenant_code=tenant_code,
                statement_id=r["statement_id"],
                analysis_type="statement_category",
                analysis_column=[r["statement_type"]],
                ml_model_name=settings.SETFIT_MODEL_ID,
                ml_model_version=settings.SETFIT_MODEL_VERSION,
                model_confidence_score=r["model_conf"],
                model_prediction=r["model_pred"],
                llm_confidence_score=r["llm_conf"],
                llm_prediction=r["llm_pred"],
                threshold=threshold,
                justification=r["justification"],
            )

    model_only_count = sum(1 for r in results if not r["llm_pred"])
    llm_fallback_count = sum(1 for r in results if r["llm_pred"])

    logger.info(
        "Statement categorization complete for submission=%s: %d total, %d model-only, %d LLM-fallback",
        submission_id, len(results), model_only_count, llm_fallback_count,
    )

    return {
        "status": "success",
        "total": len(results),
        "model_only": model_only_count,
        "llm_fallback": llm_fallback_count,
    }
