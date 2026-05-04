"""
LLM Book Summarization Operator
────────────────────────────────────────────────────────────────────────────────
Uses HuggingFace InferenceClient to generate a
human-readable Italian summary for each book record and writes it to
the PostgreSQL `book_descriptions.summary` column.

Two tasks are exposed:
  1. summarize_books_task  – pure Python function, callable standalone
  2. LLMSummarizerOperator – Airflow BaseOperator wrapper

Based on your existing prompt-engineering work in prova_prompts.ipynb.
────────────────────────────────────────────────────────────────────────────────
"""

import json
import logging
import re
import time
import os
from typing import Optional

import psycopg2
import psycopg2.extras
from airflow.models import BaseOperator

log = logging.getLogger(__name__)

# ── Connection ───────────────────────────────────────────────────────────────
# Import the connection logic (including SSH tunneling) from our database_operator
from operators.database_operator import _get_conn


# ═══════════════════════════════════════════════════════════════════════════════
# PROMPT TEMPLATE
# ═══════════════════════════════════════════════════════════════════════════════

SUMMARY_PROMPT_IT = """Sei un bibliotecario esperto di manoscritti e libri antichi.
Ti vengono forniti i metadati catalogici di un libro della collezione digitale MAGIC
dell'Università degli Studi di Napoli Federico II.

Scrivi una descrizione sintetica e accademica in italiano (massimo 5-6 frasi) che:
1. Presenti l'opera e l'autore in modo contestualizzato.
2. Descriva le caratteristiche fisiche e tipografiche essenziali.
3. Menzioni lo stato di conservazione e le peculiarità decorative.
4. Citi eventuali nomi significativi legati alla storia del volume.

NON inventare informazioni non presenti nei metadati.
NON usare elenchi puntati: scrivi solo prosa fluente.
Rispondi SOLO con il testo della descrizione, senza intestazioni né commenti.

Metadati del libro:
{metadata_block}

Descrizione:"""


SUMMARY_PROMPT_EN = """You are an expert librarian specialising in rare books and manuscripts.
You are given cataloguing metadata for a book from the MAGIC digital collection
at the University of Naples Federico II (Italy).

Write a concise academic description in English (maximum 5-6 sentences) that:
1. Introduces the work and its author with scholarly context.
2. Describes the essential physical and typographic features.
3. Mentions the conservation status and any decorative features.
4. Cites any significant names linked to the book's history.

Do NOT invent information not present in the metadata.
Do NOT use bullet points: write flowing prose only.
Reply ONLY with the description text, no headings or commentary.

Book metadata:
{metadata_block}

Description:"""


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _format_metadata_block(data: dict) -> str:
    """
    Convert the extracted book dict into a clean Italian key: value block
    for injection into the prompt.
    """
    # Human-readable Italian labels (matches FIELD_MAP in data_extractor.py)
    label_map = {
        "author":               "Autore",
        "second_author":        "Autore secondario",
        "title":                "Titolo",
        "publication":          "Pubblicazione",
        "dimensions":           "Dimensioni",
        "weight":               "Peso",
        "thickness":            "Spessore dei fogli",
        "location":             "Collocazione",
        "signature":            "Segnatura",
        "imprint":              "Impronta",
        "text_layout":          "Disposizione del testo",
        "lines":                "Righe",
        "requests":             "Richiami",
        "binding":              "Legatura",
        "language_info":        "Lingua",
        "significant_names":    "Nomi significativi",
        "condition_info":       "Stato di conservazione",
        "decoration":           "Decorazione",
        "physical_description": "Descrizione fisica",
    }

    lines = []
    for field, label in label_map.items():
        value = data.get(field, "")
        if not value:
            continue
        # Strip HTML tags (hyperlinks stored in DB)
        clean = re.sub(r"<[^>]+>", "", str(value)).strip()
        if clean:
            lines.append(f"{label}: {clean}")

    return "\n".join(lines)


def _call_huggingface(prompt: str, model: str, token: str) -> str:
    """
    Call HuggingFace Inference API.
    """
    try:
        from huggingface_hub import InferenceClient
        client = InferenceClient(token=token)
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=400,
        )
        return response.choices[0].message.content.strip()
    except Exception as exc:
        raise RuntimeError(f"HuggingFace call failed ({model}): {exc}") from exc





# ═══════════════════════════════════════════════════════════════════════════════
# SUMMARY QUALITY SCORING
# ═══════════════════════════════════════════════════════════════════════════════

def score_summary(summary: str, metadata: dict, language: str = "it") -> int:
    """
    Score a generated summary on a 0–100 scale using local heuristics.
    No extra API calls required.

    Scoring breakdown:
      Length (25):   3–6 sentences as required by the prompt
      Prose (20):    No bullet points or numbered lists (prompt violation)
      Entities (35): Author last name + title keyword + physical attribute present
      Language (20): Lexical markers confirm expected language (Italian or English)

    Args:
        summary:  The generated summary text.
        metadata: The book metadata dict (same keys as FIELD_MAP).
        language: 'it' or 'en'.

    Returns:
        Integer score 0–100.
    """
    if not summary or not summary.strip():
        return 0

    score = 0

    # ── Length: 3–6 sentences ────────────────────────────────────────────────
    sentences = [s for s in re.split(r'[.!?]+', summary.strip()) if s.strip()]
    n = len(sentences)
    if 3 <= n <= 6:
        score += 25
    elif n == 2 or n == 7:
        score += 12  # close but off

    # ── Prose format: no bullet points or numbered lists ─────────────────────
    has_bullets = bool(re.search(r'^\s*[-•*]\s', summary, re.MULTILINE))
    has_numbered = bool(re.search(r'^\s*\d+[.)]\s', summary, re.MULTILINE))
    if not has_bullets and not has_numbered:
        score += 20

    # ── Entity coverage ───────────────────────────────────────────────────────
    entity_score = 0
    summary_lower = summary.lower()

    # Author last name (up to 12 pts)
    author = metadata.get("author", "")
    if author:
        clean_author = re.sub(r"<[^>]+>", "", author).strip()
        last_name = clean_author.split()[-1] if clean_author.split() else ""
        if last_name and last_name.lower() in summary_lower:
            entity_score += 12

    # First significant title word (up to 12 pts)
    title = metadata.get("title", "")
    if title:
        clean_title = re.sub(r"<[^>]+>", "", title).strip()
        title_words = [w for w in clean_title.split() if len(w) > 4]
        if title_words and title_words[0].lower() in summary_lower:
            entity_score += 12

    # Physical attribute present in any of: dimensions, weight, binding, condition (11 pts)
    physical_fields = ["dimensions", "weight", "binding", "condition_info"]
    for field in physical_fields:
        raw = metadata.get(field, "")
        if not raw:
            continue
        clean = re.sub(r"<[^>]+>", "", str(raw)).strip()
        words = [w for w in clean.split() if len(w) > 3]
        if any(w.lower() in summary_lower for w in words):
            entity_score += 11
            break

    score += entity_score

    # ── Language markers ──────────────────────────────────────────────────────
    if language == "it":
        markers = ["della", "del", "il", "la", "le", "di", "un", "una",
                   "con", "che", "nel", "gli", "dei", "alle", "sono", "questo"]
    else:
        markers = ["the", "of", "and", "is", "in", "with", "a", "an",
                   "that", "this", "its", "are", "was", "for"]

    found = sum(1 for m in markers if f" {m} " in f" {summary_lower} ")
    if found >= 5:
        score += 20
    elif found >= 2:
        score += 10

    return min(score, 100)


# ═══════════════════════════════════════════════════════════════════════════════
# CORE SUMMARIZATION FUNCTION
# ═══════════════════════════════════════════════════════════════════════════════

def summarize_book(
    data: dict,
    model: str = "meta-llama/Llama-3.1-8B-Instruct",
    language: str = "it",
    hf_token: str = "",
) -> dict:
    """
    Generate an LLM summary for a single book.

    Args:
        data:        Extracted book metadata dict (from DataExtractorOperator).
        model:       HuggingFace model name.
        language:    'it' (Italian) or 'en' (English).
        hf_token:    HuggingFace Inference API token.

    Returns:
        dict with keys: book_number, summary, model, latency_seconds, error
    """
    book_number = data.get("book_number", "?")
    result = {
        "book_number": book_number,
        "summary": None,
        "summary_score": None,
        "model": model,
        "language": language,
        "latency_seconds": None,
        "error": None,
    }

    metadata_block = _format_metadata_block(data)
    if not metadata_block:
        result["error"] = "No metadata to summarize"
        return result

    prompt_template = SUMMARY_PROMPT_IT if language == "it" else SUMMARY_PROMPT_EN
    prompt = prompt_template.format(metadata_block=metadata_block)

    t0 = time.time()
    try:
        if not hf_token:
            raise ValueError("HUGGINGFACEHUB_API_TOKEN is not set")
            
        summary = _call_huggingface(prompt, model, token=hf_token)
        quality_score = score_summary(summary, data, language)

        result["summary"] = summary
        result["summary_score"] = quality_score
        result["latency_seconds"] = round(time.time() - t0, 2)
        log.info(
            "[%s] summary generated in %.2fs (%d chars) score=%d/100 — model: %s",
            book_number, result["latency_seconds"], len(summary), quality_score, model,
        )
    except Exception as exc:
        result["error"] = str(exc)
        result["latency_seconds"] = round(time.time() - t0, 2)
        log.error("[%s] summarization failed: %s", book_number, exc)

    return result


def write_summary_to_db(
    book_number: str,
    collection_id: int,
    summary: str,
    language: str = "it",
    summary_score: int = None,
) -> bool:
    """
    Upsert the ``summary`` and ``summary_score`` columns in book_descriptions.

    Requires the columns to exist (run once on the remote DB):
        ALTER TABLE book_descriptions ADD COLUMN IF NOT EXISTS summary TEXT;
        ALTER TABLE book_descriptions ADD COLUMN IF NOT EXISTS summary_score SMALLINT;
    """
    try:
        conn = _get_conn()
        cur = conn.cursor()

        # Find the description row
        cur.execute(
            """
            SELECT bd.description_id
            FROM book_descriptions bd
            JOIN books b ON b.book_id = bd.book_id
            WHERE UPPER(b.number) = %s
              AND bd.collection_id = %s
              AND bd.language = %s
            LIMIT 1
            """,
            (book_number.upper(), collection_id, language),
        )
        row = cur.fetchone()

        def _update(desc_id):
            cur.execute(
                """UPDATE book_descriptions
                      SET summary = %s, summary_score = %s
                    WHERE description_id = %s""",
                (summary, summary_score, desc_id),
            )

        if row:
            _update(row[0])
            action = "updated"
        else:
            # Fall back: try matching on number alone
            cur.execute(
                "SELECT description_id FROM book_descriptions WHERE UPPER(number) = %s AND language = %s LIMIT 1",
                (book_number.upper(), language),
            )
            row = cur.fetchone()
            if row:
                _update(row[0])
                action = "updated (fallback)"
            else:
                log.warning("No description row found for %s", book_number)
                conn.close()
                return False

        conn.commit()
        conn.close()
        score_str = f"  score={summary_score}/100" if summary_score is not None else ""
        log.info("Summary %s in DB for %s%s", action, book_number, score_str)
        return True

    except Exception as exc:
        log.error("DB write failed for %s: %s", book_number, exc)
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# BATCH FUNCTION (callable standalone or from DAG)
# ═══════════════════════════════════════════════════════════════════════════════

def summarize_books_task(
    books_data: list,
    model: str = "Qwen/Qwen2.5-72B-Instruct",
    language: str = "it",
    write_to_db: bool = True,
    hf_token: str = "",
) -> dict:
    """
    Summarize all books in the batch and optionally persist to DB.

    Returns a summary report dict suitable for XCom.
    """
    log.info("LLM SUMMARIZER  model=%s  lang=%s", model, language)

    report = {
        "total": len(books_data),
        "success": 0,
        "failed": 0,
        "skipped": 0,
        "low_quality": [],   # book numbers with summary_score < 60
        "results": [],
        "model": model,
        "language": language,
    }

    for data in books_data:
        book_number = data.get("book_number", "?")
        log.info("Summarizing: %s", book_number)

        result = summarize_book(data, model=model, language=language, hf_token=hf_token)
        report["results"].append(result)

        if result["error"]:
            report["failed"] += 1
            continue

        # Flag low-quality summaries (score < 60) for email notification
        score = result.get("summary_score")
        if score is not None and score < 60:
            report["low_quality"].append({"book_number": book_number, "score": score})

        if write_to_db and result["summary"]:
            ok = write_summary_to_db(
                book_number=book_number,
                collection_id=data.get("collection_id", 0),
                summary=result["summary"],
                language=language,
                summary_score=result.get("summary_score"),
            )
            if ok:
                report["success"] += 1
            else:
                report["failed"] += 1
        else:
            report["success"] += 1

    avg_latency = (
        sum(r["latency_seconds"] for r in report["results"] if r["latency_seconds"])
        / max(report["success"] + report["failed"], 1)
    )

    log.info(
        "Summarization report — total=%d success=%d failed=%d avg_latency=%.1fs",
        report["total"], report["success"], report["failed"], avg_latency,
    )

    return report


# ═══════════════════════════════════════════════════════════════════════════════
# AIRFLOW OPERATOR
# ═══════════════════════════════════════════════════════════════════════════════

class LLMSummarizerOperator(BaseOperator):
    """
    Airflow operator that:
      1. Pulls books_data from XCom (from extract_data task).
      2. Generates an LLM summary per book via HuggingFace Inference API.
      3. Writes the summary to book_descriptions.summary in PostgreSQL.

    Usage in DAG:
        summarize = LLMSummarizerOperator(
            task_id='summarize_books',
            model='Qwen/Qwen2.5-72B-Instruct',
            language='it',
            write_to_db=True,
            hf_token=HF_TOKEN,
            dag=dag,
        )
        update_database >> summarize
    """

    template_fields = ("model", "language")

    def __init__(
        self,
        model: str = "Qwen/Qwen2.5-72B-Instruct",
        language: str = "it",
        write_to_db: bool = True,
        extract_task_id: str = "extract_data",
        hf_token: str = "",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.model = model
        self.language = language
        self.write_to_db = write_to_db
        self.extract_task_id = extract_task_id
        self.hf_token = hf_token

    def execute(self, context):
        books_data = context["task_instance"].xcom_pull(
            key="books_data",
            task_ids=self.extract_task_id,
        )

        if not books_data:
            self.log.warning("No books_data in XCom — nothing to summarize")
            return {}

        report = summarize_books_task(
            books_data=books_data,
            model=self.model,
            language=self.language,
            write_to_db=self.write_to_db,
            hf_token=self.hf_token,
        )

        # Push report to XCom for downstream tasks (e.g. email notification)
        context["task_instance"].xcom_push(key="summarization_report", value=report)
        return report["success"]
