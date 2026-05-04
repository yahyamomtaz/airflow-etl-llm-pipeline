import json
import logging
import os
import sqlite3
from datetime import datetime

log = logging.getLogger(__name__)

# ============================================================================
# PRE-VALIDATION: Check Word files BEFORE pipeline starts
# ============================================================================

def pre_validate_word_files(**context):
    """
    Validate Word files for NEW books detected by sensor.
    Supports GPT-4, Llama, or both for comparison.

    Checks:
    - File is not empty (size > 0)
    - File is readable
    - Corresponding image folder exists
    - Images present in folder
    - AI: Content validation (required fields, data quality)
    """
    from airflow.models import Variable

    log.info("PRE-VALIDATOR: Checking new books from sensor")

    new_books = context['ti'].xcom_pull(key='new_books', task_ids='wait_for_new_books')

    if not new_books:
        log.warning("No new books detected by sensor")
        return True

    log.info("Found %d new book(s) from sensor", len(new_books))

    validation_report = {
        "timestamp": datetime.now().isoformat(),
        "total_files": len(new_books),
        "valid_files": 0,
        "errors": [],
        "warnings": [],
        "ai_validations": [],
        "model_comparisons": []
    }

    use_gpt4       = bool(Variable.get('OPENAI_API_KEY', default_var=''))
    use_llama      = Variable.get('USE_LLAMA', default_var='false').lower() == 'true'
    comparison_mode = Variable.get('COMPARISON_MODE', default_var='false').lower() == 'true'
    desc_dir       = Variable.get('PIPELINE_DESC_DIR', default_var='/opt/airflow/data/descriptions')

    if comparison_mode:
        use_gpt4  = True
        use_llama = True
        log.info("COMPARISON MODE: Running both GPT-4 and Llama for research")
    elif use_gpt4 and use_llama:
        log.info("Running both GPT-4 and Llama validation")
    elif use_gpt4:
        log.info("GPT-4 validation enabled")
    elif use_llama:
        log.info("Llama validation enabled")
    else:
        log.warning("No AI models enabled, skipping content validation")

    if use_gpt4 or use_llama:
        try:
            from .model_comparison import compare_model_validations
        except ImportError:
            from model_comparison import compare_model_validations

    for book_info in new_books:
        filename    = book_info['docx_file']
        book_number = book_info['book_number']
        file_path   = os.path.join(desc_dir, filename)

        log.info("Validating: %s (Book: %s)", filename, book_number)

        if not os.path.exists(file_path):
            error = f"{filename}: File not found"
            validation_report["errors"].append(error)
            log.error(error)
            continue

        file_size = os.path.getsize(file_path)
        if file_size == 0:
            error = f"{filename}: File is empty (0 bytes)"
            validation_report["errors"].append(error)
            log.error(error)
            continue
        elif file_size < 1024:
            warning = f"{filename}: File very small ({file_size} bytes)"
            validation_report["warnings"].append(warning)
            log.warning(warning)

        image_folder = book_info['image_folder']
        num_images   = book_info.get('num_images', 0)

        if not os.path.exists(image_folder):
            error = f"{filename}: Image folder not found at {image_folder}"
            validation_report["errors"].append(error)
            log.error(error)
            continue

        if num_images == 0:
            error = f"{filename}: No images found in {image_folder}"
            validation_report["errors"].append(error)
            log.error(error)
            continue

        log.info("Basic checks passed (%d bytes, %d images)", file_size, num_images)

        if use_gpt4 or use_llama:
            log.info("Running AI content validation...")

            comparison_result = compare_model_validations(
                file_path, book_number, num_images,
                use_gpt4=use_gpt4,
                use_llama=use_llama,
            )

            validation_report["ai_validations"].append({
                "book_number": book_number,
                "filename":    filename,
                "results":     comparison_result,
            })

            if comparison_mode and 'comparison' in comparison_result:
                validation_report["model_comparisons"].append(comparison_result['comparison'])
                comp = comparison_result['comparison']
                log.info(
                    "Comparison — agreement: %s | latency winner: %s (%ss faster) | GPT-4 cost: $%.4f",
                    comp['agreement'],
                    comp['latency_winner'].upper(),
                    comp.get('latency_diff', 0),
                    comp.get('gpt4_cost', 0),
                )

            has_critical_issues = False

            if 'gpt4' in comparison_result:
                gpt4_result = comparison_result['gpt4']
                if not gpt4_result.get("valid", True) and not gpt4_result.get("skipped"):
                    for issue in gpt4_result.get("critical_issues", []):
                        validation_report["errors"].append(f"{filename}: GPT-4 found: {issue}")
                        log.error("GPT-4: %s", issue)
                        has_critical_issues = True

            if 'llama' in comparison_result:
                llama_result = comparison_result['llama']
                if not llama_result.get("valid", True) and not llama_result.get("skipped"):
                    for issue in llama_result.get("critical_issues", []):
                        validation_report["errors"].append(f"{filename}: Llama found: {issue}")
                        log.error("Llama: %s", issue)
                        has_critical_issues = True

            if has_critical_issues:
                continue

            log.info("AI content validation passed")

        validation_report["valid_files"] += 1

    report_path = os.path.join(
        Variable.get('PIPELINE_DATA_DIR', default_var='/opt/airflow/data'),
        'pre_validation_report.json',
    )
    with open(report_path, 'w') as f:
        json.dump(validation_report, f, indent=2)

    if comparison_mode and validation_report["model_comparisons"]:
        comparison_report_path = os.path.join(
            Variable.get('PIPELINE_DATA_DIR', default_var='/opt/airflow/data'),
            'model_comparison_report.json',
        )
        with open(comparison_report_path, 'w') as f:
            json.dump({
                "timestamp":         datetime.now().isoformat(),
                "total_comparisons": len(validation_report["model_comparisons"]),
                "comparisons":       validation_report["model_comparisons"],
                "summary":           generate_comparison_summary(validation_report["model_comparisons"]),
            }, f, indent=2)
        log.info("Model comparison report saved to: %s", comparison_report_path)

    log.info(
        "PRE-VALIDATION SUMMARY — total=%d valid=%d warnings=%d errors=%d ai_validations=%d",
        validation_report['total_files'],
        validation_report['valid_files'],
        len(validation_report['warnings']),
        len(validation_report['errors']),
        len(validation_report['ai_validations']),
    )
    log.info("Report saved to: %s", report_path)

    context['task_instance'].xcom_push(key='pre_validation_report', value=validation_report)

    if validation_report['errors']:
        log.error("PRE-VALIDATION FAILED: %d error(s) found", len(validation_report['errors']))
        raise ValueError(f"Pre-validation failed with {len(validation_report['errors'])} error(s)")

    log.info("PRE-VALIDATION PASSED")
    return True


def generate_comparison_summary(comparisons):
    """Generate summary statistics for model comparisons"""
    if not comparisons:
        return {}

    total           = len(comparisons)
    agreements      = sum(1 for c in comparisons if c.get('agreement', False))
    gpt4_faster     = sum(1 for c in comparisons if c.get('latency_winner') == 'gpt4')
    total_gpt4_cost = sum(c.get('gpt4_cost', 0) for c in comparisons)

    avg_gpt4_latency  = sum(c.get('gpt4_metrics', {}).get('latency_seconds', 0) for c in comparisons) / total
    avg_llama_latency = sum(c.get('llama_metrics', {}).get('latency_seconds', 0) for c in comparisons) / total

    return {
        "total_comparisons":       total,
        "agreement_rate":          round(agreements / total * 100, 1),
        "gpt4_faster_count":       gpt4_faster,
        "llama_faster_count":      total - gpt4_faster,
        "total_gpt4_cost":         round(total_gpt4_cost, 4),
        "avg_gpt4_latency_seconds": round(avg_gpt4_latency, 2),
        "avg_llama_latency_seconds": round(avg_llama_latency, 2),
        "cost_per_book":           round(total_gpt4_cost / total, 4) if total > 0 else 0,
    }


def validate_word_content_with_gpt4(file_path, book_number, num_images):
    """
    Use GPT-4 to validate the content of a Word description file.

    Returns:
        dict with keys: valid, critical_issues, warnings, suggestions, confidence
    """
    try:
        import openai
        from docx import Document
        from airflow.models import Variable

        api_key = Variable.get('OPENAI_API_KEY', default_var='')
        if not api_key:
            return {"valid": True, "error": "OPENAI_API_KEY Variable not set", "skipped": True}
        openai.api_key = api_key

        doc       = Document(file_path)
        full_text = "\n".join([p.text for p in doc.paragraphs if p.text.strip()])
        if len(full_text) > 4000:
            full_text = full_text[:4000] + "\n... (truncated)"

        validation_prompt = f"""You are validating a book description file for a digital library pipeline.

**Book Information:**
- Book Number: {book_number}
- Number of Images: {num_images}

**Description File Content:**
```
{full_text}
```

**Validation Requirements:**
Check if the description contains:
1. Author name (required)
2. Title (required)
3. Publication info (required)
4. Physical dimensions
5. Location/shelf mark
6. Language information
7. Condition notes

Return JSON ONLY (no markdown):
{{
  "valid": true/false,
  "critical_issues": ["list of critical missing fields"],
  "warnings": ["list of warnings"],
  "suggestions": ["improvement suggestions"],
  "confidence": 0-100,
  "detected_fields": {{
    "author": "extracted author name or null",
    "title": "extracted title or null",
    "publication": "extracted publication info or null"
  }}
}}"""

        response    = openai.chat.completions.create(
            model="gpt-4-turbo",
            messages=[
                {"role": "system", "content": "You are a meticulous digital library QA expert. Return ONLY valid JSON."},
                {"role": "user",   "content": validation_prompt},
            ],
            temperature=0.1,
            response_format={"type": "json_object"},
        )
        return json.loads(response.choices[0].message.content)

    except ImportError:
        return {"valid": True, "error": "docx or openai package not installed", "skipped": True}
    except json.JSONDecodeError as e:
        return {"valid": True, "error": f"GPT-4 returned invalid JSON: {e}", "skipped": True}
    except Exception as e:
        return {"valid": True, "error": str(e), "skipped": True}


def validate_pipeline_output(books_data, **context):
    """
    Validates the entire pipeline output for all books.

    Checks metadata consistency, manifest structure, viewer files, and DB entries.
    """
    from airflow.models import Variable

    log.info("AI VALIDATOR: Checking pipeline outputs")

    validation_report = {
        "timestamp":   datetime.now().isoformat(),
        "total_books": len(books_data),
        "validated":   0,
        "passed":      0,
        "warnings":    [],
        "errors":      [],
        "book_reports": [],
    }

    for data in books_data:
        book_number = data.get('book_number', 'Unknown')
        log.info("Validating book: %s", book_number)

        book_report = validate_single_book(data)
        validation_report["book_reports"].append(book_report)
        validation_report["validated"] += 1

        if book_report["status"] == "passed":
            validation_report["passed"] += 1
            log.info("Validation PASSED for %s", book_number)
        else:
            log.warning("Validation FAILED for %s", book_number)
            for error in book_report.get("errors", []):
                log.error("  %s", error)
                validation_report["errors"].append(f"{book_number}: {error}")

        for warning in book_report.get("warnings", []):
            log.warning("  %s", warning)
            validation_report["warnings"].append(f"{book_number}: {warning}")

    report_path = os.path.join(
        Variable.get('PIPELINE_DATA_DIR', default_var='/opt/airflow/data'),
        'validation_report.json',
    )
    with open(report_path, 'w') as f:
        json.dump(validation_report, f, indent=2)

    log.info(
        "VALIDATION SUMMARY — total=%d passed=%d failed=%d warnings=%d errors=%d",
        validation_report['total_books'],
        validation_report['passed'],
        validation_report['total_books'] - validation_report['passed'],
        len(validation_report['warnings']),
        len(validation_report['errors']),
    )
    log.info("Full report saved to: %s", report_path)

    context['task_instance'].xcom_push(key='validation_report', value=validation_report)
    return len(validation_report['errors']) == 0


def validate_single_book(data):
    """Validate a single book's pipeline output."""
    from airflow.models import Variable

    book_number = data.get('book_number', 'Unknown').lower()
    author_slug = data.get('author_slug', 'unknown').lower()
    data_dir    = Variable.get('PIPELINE_DATA_DIR', default_var='/opt/airflow/data')

    errors        = []
    warnings      = []
    checks_passed = []

    # 1. Manifest
    manifest_path = os.path.join(data_dir, 'manifests', book_number, 'manifest.json')
    if os.path.exists(manifest_path):
        checks_passed.append("manifest_exists")
        try:
            with open(manifest_path, 'r') as f:
                manifest = json.load(f)

            if "@context" not in manifest:
                errors.append("Manifest missing @context")
            elif manifest["@context"] != "http://iiif.io/api/presentation/2/context.json":
                warnings.append("Manifest using non-standard IIIF context")

            if "@type" not in manifest or manifest["@type"] != "sc:Manifest":
                errors.append("Manifest missing or incorrect @type")

            if "sequences" not in manifest or not manifest["sequences"]:
                errors.append("Manifest has no sequences")
            elif not manifest["sequences"][0].get("canvases"):
                errors.append("Manifest has no canvases (no images)")
            else:
                num_canvases = len(manifest["sequences"][0]["canvases"])
                checks_passed.append(f"manifest_has_{num_canvases}_canvases")

                books_dir    = Variable.get('PIPELINE_BOOKS_DIR', default_var=os.path.join(data_dir, 'books'))
                image_folder = os.path.join(books_dir, book_number)
                if os.path.exists(image_folder):
                    num_actual = len([f for f in os.listdir(image_folder) if f.endswith('.jpg')])
                    if num_actual != num_canvases:
                        errors.append(
                            f"Image count mismatch: {num_actual} images in folder "
                            f"but {num_canvases} canvases in manifest"
                        )
                        log.error("CRITICAL: Image/Canvas mismatch — folder=%d manifest=%d",
                                  num_actual, num_canvases)
                    else:
                        checks_passed.append(f"image_count_matches_{num_actual}")
                        log.info("Image count matches: %d", num_actual)
                else:
                    warnings.append(f"Could not verify image count: folder {image_folder} not found")

            if "label" in manifest:
                expected_label = f"{book_number}-{author_slug}"
                if manifest["label"] != expected_label:
                    warnings.append(
                        f"Manifest label '{manifest['label']}' doesn't match expected '{expected_label}'"
                    )

            checks_passed.append("manifest_valid_structure")

        except json.JSONDecodeError:
            errors.append("Manifest is not valid JSON")
        except Exception as e:
            errors.append(f"Error reading manifest: {e}")
    else:
        errors.append(f"Manifest file not found: {manifest_path}")

    # 2. Viewer file
    viewer_path = os.path.join(data_dir, 'viewers', f"{book_number}-{author_slug}-viewer.js")
    if os.path.exists(viewer_path):
        checks_passed.append("viewer_exists")
        try:
            with open(viewer_path, 'r') as f:
                viewer_content = f.read()

            if "MiradorViewer" not in viewer_content:
                errors.append("Viewer missing MiradorViewer component")
            if f"Viewer{book_number}" not in viewer_content:
                warnings.append("Viewer component name doesn't match expected pattern")

            expected_url = f"/collections/{data.get('collection_name', '').lower()}/{book_number}-{author_slug}/manifest.json"
            if expected_url not in viewer_content:
                warnings.append("Viewer manifest URL might be incorrect")

            checks_passed.append("viewer_valid_content")
        except Exception as e:
            errors.append(f"Error reading viewer: {e}")
    else:
        errors.append(f"Viewer file not found: {viewer_path}")

    # 3. Database entry (SQLite local check — informational only)
    try:
        db_path = os.path.join(data_dir, 'collections.db')
        conn    = sqlite3.connect(db_path)
        cursor  = conn.cursor()
        cursor.execute("SELECT * FROM books WHERE number = ?", (data.get('book_number'),))
        book_row = cursor.fetchone()
        if book_row:
            checks_passed.append("database_entry_exists")
            cursor.execute("SELECT * FROM book_descriptions WHERE book_id = ?", (book_row[0],))
            if cursor.fetchone():
                checks_passed.append("database_description_exists")
            else:
                warnings.append("Book exists but has no description entry")
        else:
            errors.append("Book not found in database")
        conn.close()
    except sqlite3.Error as e:
        errors.append(f"Database error: {e}")
    except Exception as e:
        warnings.append(f"Could not check database: {e}")

    # 4. Metadata checks
    if not data.get('author_slug') or data.get('author_slug') == 'unknown-author':
        warnings.append("Author slug is missing or unknown")
    if not data.get('book_number'):
        errors.append("Book number is missing")

    status = "passed" if not errors else "failed"
    return {
        "book_number":   data.get('book_number'),
        "status":        status,
        "checks_passed": checks_passed,
        "warnings":      warnings,
        "errors":        errors,
        "timestamp":     datetime.now().isoformat(),
    }


def validate_with_llm(data, manifest, viewer_content, **context):
    """Use GPT-4 for advanced validation (optional — requires OPENAI_API_KEY Variable)."""
    try:
        import openai
        from airflow.models import Variable

        api_key = Variable.get('OPENAI_API_KEY', default_var='')
        if not api_key:
            return {"error": "OPENAI_API_KEY Variable not set", "skipped": True}
        openai.api_key = api_key

        validation_prompt = f"""You are validating digital library pipeline output.

**Book Metadata:**
- Book Number: {data.get('book_number')}
- Author: {data.get('author')}
- Title: {data.get('title')}
- Collection: {data.get('collection_name')}

**Generated Manifest (IIIF 2.0):**
```json
{json.dumps(manifest, indent=2)[:1000]}... (truncated)
```

**Generated Viewer Component:**
```javascript
{viewer_content[:500]}... (truncated)
```

Return JSON:
{{
  "valid": true/false,
  "confidence": 0-100,
  "issues": ["list of issues found"],
  "suggestions": ["improvement suggestions"]
}}"""

        response = openai.chat.completions.create(
            model="gpt-4-turbo",
            messages=[
                {"role": "system", "content": "You are a digital library QA expert. Be thorough but concise."},
                {"role": "user",   "content": validation_prompt},
            ],
            temperature=0.2,
        )
        return json.loads(response.choices[0].message.content)

    except ImportError:
        return {"error": "openai package not installed", "skipped": True}
    except Exception as e:
        return {"error": str(e), "skipped": True}
