"""
Book Publication Pipeline DAG  ── WITH LLM QUALITY + SUMMARIZATION
────────────────────────────────────────────────────────────────────────────────
Flow:
  1. Sensor          – detect new Word docs + image folders
  2. Extract         – parse Word documents → XCom
  3. LLM Validate    – Ollama checks Author / Dimensioni / Peso formatting
  4. Write to DB     – upsert books + descriptions into PostgreSQL
  5. LLM Summarize   – Ollama writes summary → book_descriptions.summary
  6. Manifests       – generate IIIF manifest.json files locally
  7. Transfer        – push images + manifests to remote image server via SSH
  8. Mark done       – update Airflow Variable tracking state
  9. Email + Slack   – notify team

────────────────────────────────────────────────────────────────────────────────
"""

import logging
import os
import re
import sys

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

sys.path.insert(0, '/opt/airflow')

from operators.sensors.file_sensor import NewBookSensor
from operators.data_extractor import extract_data_from_docx
from operators.database_operator import update_db_from_docx, DatabaseUpdateOperator
from operators.manifest_generator_operator import ManifestGeneratorOperator
from operators.processed_books import (
    mark_books_as_processed,
    update_book_status,
    load_processing_status,
)
from operators.email_notification_operator import EmailNotificationOperator
from operators.llm_summarizer_operator import LLMSummarizerOperator, summarize_books_task
from operators.s3_upload_operator import S3UploadOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

log = logging.getLogger(__name__)

# ─── Paths (configurable via Airflow Variables for shared/S3 mounts) ──────────
DESC_DIR       = Variable.get('PIPELINE_DESC_DIR',      default_var='/opt/airflow/data/descriptions')
OUTPUT_BASE    = Variable.get('PIPELINE_BOOKS_DIR',     default_var='/opt/airflow/data/books')
MANIFESTS_BASE = Variable.get('PIPELINE_MANIFESTS_DIR', default_var='/opt/airflow/data/manifests')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'library.book_ingest',
    default_args=default_args,
    description='Extract Word → LLM Validate → PostgreSQL → LLM Summarize → Manifests → Transfer → Notify',
    schedule='@hourly',
    catchup=False,
    tags=['library', 'books', 'ingest', 'llm'],
)


# ════════════════════════════════════════════════════════════════════════════
# STEP 1 — Sensor
# ════════════════════════════════════════════════════════════════════════════
wait_for_books = NewBookSensor(
    task_id='wait_for_new_books',
    poke_interval=60,
    timeout=3600,
    mode='reschedule',
    soft_fail=True,
    dag=dag,
)


# ════════════════════════════════════════════════════════════════════════════
# STEP 2 — Extract
# ════════════════════════════════════════════════════════════════════════════
def extract_all_books(**context):
    desc_dir   = Variable.get('PIPELINE_DESC_DIR',  default_var='/opt/airflow/data/descriptions')
    output_base = Variable.get('PIPELINE_BOOKS_DIR', default_var='/opt/airflow/data/books')

    log.info("STEP 2: Extracting data from Word files")

    new_books = context['ti'].xcom_pull(key='new_books', task_ids='wait_for_new_books')

    extracted_data     = []
    failed_extractions = []

    if not new_books:
        log.warning("No new books from sensor – scanning all .docx files")
        docx_files = [f for f in os.listdir(desc_dir)
                      if f.lower().endswith('.docx') and not f.startswith('~')]
        if not docx_files:
            raise FileNotFoundError(f"No .docx files found in {desc_dir}")
        source = [{'docx_file': f, 'book_number': '?'} for f in docx_files]
    else:
        log.info("Processing %d new book(s)", len(new_books))
        source = new_books

    for book_info in source:
        file_path   = os.path.join(desc_dir, book_info['docx_file'])
        book_number = book_info.get('book_number', '?')
        log.info("Processing: %s", book_info['docx_file'])
        try:
            data = extract_data_from_docx(file_path)
            if not data:
                msg = "No data returned"
                failed_extractions.append({'file': book_info['docx_file'], 'reason': msg})
                update_book_status(book_number, 'extracted', False, msg)
                continue

            if book_info.get('image_folder'):
                actual_folder = os.path.basename(book_info['image_folder'])
                data['image_folder_name'] = actual_folder
                m = re.match(r'^\d+-(.+)$', actual_folder)
                if m:
                    folder_title = m.group(1)
                    data['book_number'] = folder_title
                    data['book_slug'] = re.sub(r'[^a-z0-9]+', '-', folder_title.lower()).strip('-')
            elif data.get('image_folder_name'):
                prefix_match = re.match(r'^(\d+)-', data['image_folder_name'])
                if prefix_match and not os.path.isdir(os.path.join(output_base, data['image_folder_name'])):
                    prefix = prefix_match.group(1)
                    try:
                        actual = next(
                            (e for e in os.listdir(output_base)
                             if e.startswith(f'{prefix}-') and os.path.isdir(os.path.join(output_base, e))),
                            None,
                        )
                        if actual:
                            data['image_folder_name'] = actual
                            m = re.match(r'^\d+-(.+)$', actual)
                            if m:
                                folder_title = m.group(1)
                                data['book_number'] = folder_title
                                data['book_slug'] = re.sub(r'[^a-z0-9]+', '-', folder_title.lower()).strip('-')
                    except OSError:
                        pass

            is_simple = bool(data.get('image_folder_name'))
            required = ['book_number', 'collection_name'] + ([] if is_simple else ['author_slug'])
            missing = [f for f in required if not data.get(f)]
            if missing:
                msg = f"Missing required fields: {missing}"
                failed_extractions.append({'file': book_info['docx_file'], 'reason': msg})
                update_book_status(book_number, 'extracted', False, msg)
                continue

            extracted_data.append(data)
            log.info("Extracted: %s", data.get('book_number'))
            update_book_status(data['book_number'], 'extracted', True)

        except Exception as e:
            log.error("Error extracting %s: %s", book_info['docx_file'], e)
            failed_extractions.append({'file': book_info['docx_file'], 'reason': str(e)})
            update_book_status(book_number, 'extracted', False, str(e))

    if failed_extractions:
        context['ti'].xcom_push(key='failed_extractions', value=failed_extractions)

    context['ti'].xcom_push(key='books_data', value=extracted_data)
    log.info("Extracted %d OK, %d failed", len(extracted_data), len(failed_extractions))

    if not extracted_data:
        raise ValueError(
            f"All {len(failed_extractions)} book(s) failed extraction. "
            f"Details: {failed_extractions}"
        )

    return len(extracted_data)


extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_all_books,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)


# ════════════════════════════════════════════════════════════════════════════
# STEP 3 — LLM VALIDATION
# ════════════════════════════════════════════════════════════════════════════
def llm_validate_books(**context):
    """
    Use HuggingFace Inference API to validate and auto-correct three noisy fields:
      • Autore       – name format 'Cognome, Nome'
      • Dimensioni   – physical plausibility (no 4000mm heights)
      • Peso         – weight plausibility in grams
    """
    import re as _re
    try:
        from huggingface_hub import InferenceClient
        has_hf = True
    except ImportError:
        has_hf = False

    llm_model = Variable.get('LLM_MODEL', default_var='Qwen/Qwen2.5-72B-Instruct')
    hf_token  = Variable.get('HUGGINGFACEHUB_API_TOKEN', default_var='')

    log.info("STEP 3: LLM Validation  model=%s", llm_model)

    books_data = context['ti'].xcom_pull(key='books_data', task_ids='extract_data')
    if not books_data:
        log.warning("No books_data – skipping validation")
        return 0

    if not has_hf:
        log.warning("huggingface_hub package not installed – skipping LLM validation")
        return 0
    if not hf_token:
        log.warning("HUGGINGFACEHUB_API_TOKEN Variable not set – skipping LLM validation")
        return 0

    client = InferenceClient(token=hf_token)

    prompt_author = """Prendi i seguenti autori e controlla che siano scritti con le iniziali \
maiuscole e con cognome e nome separati da virgola. Se ci sono più autori, separali con ';'. \
Restituisci SOLO il testo corretto, senza commenti.

Esempi:
Input: "manzoni alessandro" -> Output: Manzoni, Alessandro
Input: "Plinius Secundus, Gaius;Gelen, Sigismund" -> Output: Plinius Secundus, Gaius; Gelen, Sigismund

Testo: {text}
Output:"""

    prompt_dimensioni = """Analizza le dimensioni del libro.
REGOLE OBBLIGATORIE:
1. Restituisci SEMPRE e SOLTANTO la stringa delle dimensioni (es: 160 x 115 x 31 mm).
2. Se le dimensioni sono corrette, riscrivile esattamente.
3. Se rilevi un errore palese (es: 4000mm invece di 400mm), correggi e scrivi solo il risultato.
4. NON aggiungere commenti.

Esempi:
Input: "4090 x 282 x 52 mm (h x l x p)" -> Output: 409 x 282 x 52 mm (h x l x p)
Input: "380 x 261 x 73 mm (h x l x p)"  -> Output: 380 x 261 x 73 mm (h x l x p)

Testo: {text}
Output:"""

    prompt_peso = """Analizza il peso del libro.
REGOLE:
1. Restituisci SEMPRE il peso espresso in grammi (g).
2. Se vedi errori palesi di scala (es. 37100g invece di 3710g), correggili.
3. Se il dato è corretto, riscrivilo esattamente.
4. NON aggiungere commenti.

Esempi:
Input: "25000 g" -> Output: 2500 g
Input: "3518 g"  -> Output: 3518 g

Testo: {text}
Output:"""

    def ask_huggingface(prompt_template, text):
        prompt = prompt_template.format(text=text)
        try:
            resp = client.chat.completions.create(
                model=llm_model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=80,
            )
            return resp.choices[0].message.content.strip()
        except Exception as exc:
            log.warning("LLM call failed: %s", exc)
            return text

    corrections_log = []

    for data in books_data:
        book_number = data.get("book_number", "?")
        log.info("Validating: %s", book_number)

        if data.get("author"):
            corrected = ask_huggingface(prompt_author, data["author"])
            if corrected and corrected != data["author"]:
                log.info("Author corrected: '%s' -> '%s'", data['author'], corrected)
                corrections_log.append({"book": book_number, "field": "author",
                                        "original": data["author"], "corrected": corrected})
                data["author"] = corrected

        if data.get("dimensions"):
            corrected = ask_huggingface(prompt_dimensioni, data["dimensions"])
            if corrected and corrected != data["dimensions"]:
                log.info("Dimensions corrected: '%s' -> '%s'", data['dimensions'], corrected)
                corrections_log.append({"book": book_number, "field": "dimensions",
                                        "original": data["dimensions"], "corrected": corrected})
                data["dimensions"] = corrected

        if data.get("weight"):
            corrected = ask_huggingface(prompt_peso, data["weight"])
            if corrected and corrected != data["weight"]:
                log.info("Weight corrected: '%s' -> '%s'", data['weight'], corrected)
                corrections_log.append({"book": book_number, "field": "weight",
                                        "original": data["weight"], "corrected": corrected})
                data["weight"] = corrected

        log.info("Validated: %s", book_number)

    context['ti'].xcom_push(key='books_data', value=books_data)
    context['ti'].xcom_push(key='validation_corrections', value=corrections_log)

    log.info("Validated %d books | %d field(s) auto-corrected", len(books_data), len(corrections_log))
    return len(corrections_log)


llm_validate = PythonOperator(
    task_id='llm_validate',
    python_callable=llm_validate_books,
    dag=dag,
)


# ════════════════════════════════════════════════════════════════════════════
# STEP 4 — Write to PostgreSQL
# ════════════════════════════════════════════════════════════════════════════
def write_to_postgres(**context):
    log.info("STEP 4: Writing data to PostgreSQL")

    books_data = context['ti'].xcom_pull(key='books_data', task_ids='llm_validate')
    if not books_data:
        books_data = context['ti'].xcom_pull(key='books_data', task_ids='extract_data')
    if not books_data:
        raise ValueError("No books data available to write to PostgreSQL")

    success  = 0
    failures = []
    for data in books_data:
        try:
            if update_db_from_docx(data):
                success += 1
                update_book_status(data['book_number'], 'db_written', True)
            else:
                msg = 'update_db_from_docx returned False'
                failures.append({'book': data['book_number'], 'reason': msg})
                update_book_status(data['book_number'], 'db_written', False, msg)
        except Exception as e:
            log.error("DB error for %s: %s", data.get('book_number'), e)
            failures.append({'book': data.get('book_number', '?'), 'reason': str(e)})
            update_book_status(data.get('book_number', '?'), 'db_written', False, str(e))

    log.info("Wrote %d/%d books to PostgreSQL", success, len(books_data))

    if success == 0:
        raise RuntimeError(
            f"All {len(books_data)} DB write(s) failed. Details: {failures}"
        )

    return success


update_database = PythonOperator(
    task_id='write_to_postgres',
    python_callable=write_to_postgres,
    dag=dag,
)


# ════════════════════════════════════════════════════════════════════════════
# STEP 5 — LLM SUMMARIZATION
# ════════════════════════════════════════════════════════════════════════════
summarize_books = LLMSummarizerOperator(
    task_id='llm_summarize',
    model=Variable.get('LLM_MODEL',      default_var='Qwen/Qwen2.5-72B-Instruct'),
    language=Variable.get('LLM_LANGUAGE', default_var='it'),
    write_to_db=True,
    extract_task_id='llm_validate',
    hf_token=Variable.get('HUGGINGFACEHUB_API_TOKEN', default_var=''),
    dag=dag,
)


# ════════════════════════════════════════════════════════════════════════════
# STEP 6 — Generate IIIF manifests
# ════════════════════════════════════════════════════════════════════════════
generate_manifests = ManifestGeneratorOperator(
    task_id='generate_manifests',
    books_dir=OUTPUT_BASE,
    manifests_dir=MANIFESTS_BASE,
    dag=dag,
)


# ════════════════════════════════════════════════════════════════════════════
# STEP 7 — Upload files to S3
# ════════════════════════════════════════════════════════════════════════════
def prepare_s3_upload_list(**context):
    output_base    = Variable.get('PIPELINE_BOOKS_DIR',     default_var='/opt/airflow/data/books')
    manifests_base = Variable.get('PIPELINE_MANIFESTS_DIR', default_var='/opt/airflow/data/manifests')

    books_data = context['ti'].xcom_pull(key='books_data', task_ids='llm_validate')
    if not books_data:
        books_data = context['ti'].xcom_pull(key='books_data', task_ids='extract_data')

    status_data = load_processing_status()
    upload_list = []

    if not books_data:
        log.warning("No books data — skipping S3 upload preparation")
        context['ti'].xcom_push(key='s3_upload_list', value=[])
        return []

    for data in books_data:
        book_number     = data.get('book_number')
        collection_name = data.get('collection_name', 'unknown')
        author_slug     = data.get('author_slug', '')

        if data.get('book_slug'):
            book_slug = data['book_slug']
        elif author_slug:
            book_slug = f"{book_number.lower()}-{author_slug.lower()}"
        else:
            book_slug = book_number.lower() if book_number else None

        if not all([book_number, collection_name, book_slug]):
            continue

        book_status = status_data.get('book_status', {}).get(book_number.upper(), {})
        s3_prefix   = f"{collection_name.lower()}/{book_slug}"

        folder_name      = data.get('image_folder_name') or book_slug
        local_images_dir = os.path.join(output_base, folder_name)
        if not book_status.get('images_uploaded') and os.path.isdir(local_images_dir):
            for filename in os.listdir(local_images_dir):
                full_path = os.path.join(local_images_dir, filename)
                if os.path.isfile(full_path):
                    upload_list.append({
                        'source_path': full_path,
                        's3_key':      f"{s3_prefix}/{filename}",
                        'book_number': book_number,
                        'file_type':   'images_uploaded',
                    })

        local_manifest = os.path.join(manifests_base, book_slug, 'manifest.json')
        if not book_status.get('manifest_uploaded') and os.path.exists(local_manifest):
            upload_list.append({
                'source_path': local_manifest,
                's3_key':      f"{s3_prefix}/manifest.json",
                'book_number': book_number,
                'file_type':   'manifest_uploaded',
            })

    context['ti'].xcom_push(key='s3_upload_list', value=upload_list)
    log.info("S3 upload list ready — %d file(s)", len(upload_list))
    return upload_list


prepare_uploads = PythonOperator(
    task_id='prepare_uploads',
    python_callable=prepare_s3_upload_list,
    dag=dag,
)

upload_to_s3 = S3UploadOperator(
    task_id='upload_to_s3',
    upload_list_task_id='prepare_uploads',
    dag=dag,
)


# ════════════════════════════════════════════════════════════════════════════
# STEP 8 — Mark processed
# ════════════════════════════════════════════════════════════════════════════
def mark_processed(**context):
    from datetime import date

    books_data = context['ti'].xcom_pull(key='books_data', task_ids='llm_validate')
    if not books_data:
        books_data = context['ti'].xcom_pull(key='books_data', task_ids='extract_data')
    try:
        if books_data:
            mark_books_as_processed([b['book_number'] for b in books_data])
    except Exception as e:
        log.warning("mark_books_as_processed failed (non-fatal): %s", e)

    data  = load_processing_status()
    today = date.today().isoformat()
    book_status = data.get('book_status', {})

    processed_today, partial_today, failed_today = [], [], []
    for book, status in book_status.items():
        if not status.get('last_updated', '').startswith(today):
            continue
        if status.get('fully_processed'):
            processed_today.append(book)
        elif any([status.get('manifest_copied'), status.get('images_copied'),
                  status.get('db_written')]):
            partial_today.append(book)
        else:
            failed_today.append(book)

    context['ti'].xcom_push(key='book_summary', value={
        'processed': processed_today,
        'partial':   partial_today,
        'failed':    failed_today,
    })
    return len(processed_today)


mark_complete = PythonOperator(
    task_id='mark_books_processed',
    python_callable=mark_processed,
    dag=dag,
)


# ════════════════════════════════════════════════════════════════════════════
# STEP 9 — Notifications (email + Slack)
# ════════════════════════════════════════════════════════════════════════════
_notification_emails = [
    e.strip()
    for e in Variable.get('NOTIFICATION_EMAILS', default_var='').split(',')
    if e.strip()
]

send_notification = EmailNotificationOperator(
    task_id='send_email_notification',
    to_emails=_notification_emails,
    subject='Nuovi libri aggiunti al sito Magic',
    include_summary=True,
    include_failed_books=True,
    trigger_rule='all_done',
    dag=dag,
)

send_slack_notification = SlackWebhookOperator(
    task_id='send_slack_notification',
    slack_webhook_conn_id='slack_alerts',
    message='Pipeline Magic completata. Controlla le email per i dettagli.',
    trigger_rule='all_done',
    dag=dag,
)

# ─── Task dependency chain ────────────────────────────────────────────────────
(
    wait_for_books
    >> extract_data
    >> llm_validate
    >> update_database
    >> summarize_books
    >> generate_manifests
    >> prepare_uploads
    >> upload_to_s3
    >> mark_complete
    >> [send_notification, send_slack_notification]
)
