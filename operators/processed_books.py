import json
import logging
from datetime import datetime

log = logging.getLogger(__name__)

VARIABLE_KEY = 'pipeline_book_status'


def load_processing_status():
    """Load current processing status from Airflow Variable."""
    from airflow.models import Variable
    raw = Variable.get(VARIABLE_KEY, default_var='{"processed_books": [], "book_status": {}}')
    return json.loads(raw)


def save_processing_status(data):
    """Save processing status to Airflow Variable."""
    from airflow.models import Variable
    Variable.set(VARIABLE_KEY, json.dumps(data))


def update_book_status(book_number, step, success, error_message=None):
    """
    Update the status of a specific processing step for a book.

    Args:
        book_number: The book ID (e.g., '5D47')
        step: The processing step (e.g., 'extracted', 'manifest_created', etc.)
        success: True if step succeeded, False otherwise
        error_message: Optional error message if step failed
    """
    data = load_processing_status()

    if book_number not in data.get('book_status', {}):
        data.setdefault('book_status', {})[book_number] = {
            'first_seen': datetime.now().isoformat(),
            'last_updated': datetime.now().isoformat(),
            'extracted': False,
            'manifest_created': False,
            'manifest_uploaded': False,
            'images_uploaded': False,
            'fully_processed': False,
            'errors': []
        }

    data['book_status'][book_number][step] = success
    data['book_status'][book_number]['last_updated'] = datetime.now().isoformat()

    if not success and error_message:
        data['book_status'][book_number]['errors'].append({
            'step': step,
            'error': error_message,
            'timestamp': datetime.now().isoformat()
        })

    status = data['book_status'][book_number]
    fully_processed = all([
        status.get('extracted', False),
        status.get('manifest_created', False),
        status.get('manifest_uploaded', False),
        status.get('images_uploaded', False),
    ])
    data['book_status'][book_number]['fully_processed'] = fully_processed

    if fully_processed and book_number not in data.get('processed_books', []):
        data.setdefault('processed_books', []).append(book_number)

    save_processing_status(data)

    log.info("%s %s: %s = %s", "OK" if success else "FAIL", book_number, step, success)


def mark_books_as_processed(book_numbers, tracking_file=None):  # tracking_file kept for backward compat
    """Mark books as processed so sensor doesn't detect them again."""
    data = load_processing_status()

    for book_number in book_numbers:
        if book_number not in data.get('processed_books', []):
            data.setdefault('processed_books', []).append(book_number)

            if book_number in data.get('book_status', {}):
                data['book_status'][book_number]['fully_processed'] = True
                data['book_status'][book_number]['last_updated'] = datetime.now().isoformat()

    save_processing_status(data)
    log.info("Marked %d books as processed", len(book_numbers))


def get_book_status(book_number):
    """Get the current status of a book"""
    data = load_processing_status()
    return data.get('book_status', {}).get(book_number, None)


def get_incomplete_books():
    """Get list of books that started processing but didn't complete all steps"""
    data = load_processing_status()
    incomplete = []
    for book_number, status in data.get('book_status', {}).items():
        if not status.get('fully_processed', False):
            incomplete.append({
                'book_number': book_number,
                'status': status
            })
    return incomplete


def print_processing_summary():
    """Log a summary of all book processing status"""
    data = load_processing_status()

    total = len(data.get('book_status', {}))
    fully_processed = sum(1 for s in data.get('book_status', {}).values() if s.get('fully_processed'))
    with_errors = sum(1 for s in data.get('book_status', {}).values() if s.get('errors'))

    log.info(
        "Processing Summary: total=%d fully_processed=%d with_errors=%d incomplete=%d",
        total, fully_processed, with_errors, total - fully_processed
    )
