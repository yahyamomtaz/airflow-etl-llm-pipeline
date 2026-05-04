from airflow.sensors.base import BaseSensorOperator
import os
from datetime import datetime
import re
from utils.storage import get_storage

class NewBookSensor(BaseSensorOperator):
    """
    Sensor that detects new books ready for processing.
    
    A book is "ready" when:
    1. Word document exists in descriptions/
    2. Corresponding image folder exists in books/
    3. Images are present in the folder
    4. Book hasn't been processed yet (checked in a tracking file)
    """
    
    def __init__(
        self,
        desc_dir='/opt/airflow/data/descriptions',
        books_dir='/opt/airflow/data/books',
        processed_tracking_file='/opt/airflow/data/processed_books.json',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.desc_dir = desc_dir
        self.books_dir = books_dir
        self.processed_tracking_file = processed_tracking_file

    @staticmethod
    def _extract_simple_prefix(filename):
        """Return NN prefix for simple-format filenames, else None."""
        match = re.match(r'^\s*(\d{2})\s*[^0-9A-Za-z]+\s*', filename)
        if not match:
            return None
        return match.group(1)
    
    def poke(self, context):
        """
        Check if there are new books ready to process.
        Return True if new books found, False otherwise.
        """
        storage = get_storage()
        self.log.info("Checking for new books [backend=%s]", storage)

        # Load list of already processed books
        processed_books = self._load_processed_books()

        # Find all Word documents
        if not storage.exists(self.desc_dir):
            self.log.warning("Descriptions directory not found: %s", self.desc_dir)
            return False

        all_files = storage.list_files(self.desc_dir)
        docx_files = [
            f for f in all_files
            if f.lower().endswith('.docx') and not f.startswith('~')
        ]

        if not docx_files:
            self.log.info("No Word documents found")
            return False
        
        # Check which books are new (not processed yet)
        new_books = []
        skipped_books = []  # Track skipped books for error reporting

        processed_books_folded = {
            re.sub(r'\s+', ' ', str(book)).strip().casefold()
            for book in processed_books
            if book
        }
        
        for docx_file in docx_files:
            # ── Simple format: "01-scheda descrittiva Compactio I.docx" ─────────
            simple_prefix = self._extract_simple_prefix(docx_file)
            if simple_prefix:
                self.log.info(
                    "Simple-format candidate: file=%r prefix=%s",
                    docx_file,
                    simple_prefix,
                )
                book_entry = self._process_simple_format_file(
                    docx_file,
                    processed_books,
                    processed_books_folded=processed_books_folded,
                    prefix=simple_prefix,
                    storage=storage,
                )
                if book_entry is None:
                    continue  # already processed or skip
                if isinstance(book_entry, dict) and 'error' in book_entry:
                    skipped_books.append({'file': docx_file, 'reason': book_entry['error']})
                    continue
                new_books.append(book_entry)
                continue

            # ── Standard format: "Scheda descrittiva_5E10_VERIFICATA.docx" ─────
            match = re.search(r'_([0-9][A-Z][0-9]+(?:\([0-9]+\))?)_', docx_file, re.IGNORECASE)
            if not match:
                # ── Generic fallback: any other .docx name ────────────────────
                self.log.info(
                    "Unrecognized filename format — trying generic fallback: %s", docx_file
                )
                book_entry = self._process_generic_format_file(
                    docx_file, processed_books_folded, storage
                )
                if book_entry is None:
                    continue
                if isinstance(book_entry, dict) and 'error' in book_entry:
                    skipped_books.append({'file': docx_file, 'reason': book_entry['error']})
                    continue
                new_books.append(book_entry)
                continue

            book_number = match.group(1)

            # Check if already processed
            if book_number.upper() in processed_books or book_number.lower() in processed_books:
                self.log.info("Book %s already processed, skipping", book_number)
                continue

            # Check if image folder exists
            actual_folder = None
            for candidate in [
                os.path.join(self.books_dir, book_number.lower()),
                os.path.join(self.books_dir, book_number.upper()),
                os.path.join(self.books_dir, book_number),
            ]:
                if storage.exists(candidate):
                    actual_folder = candidate
                    break

            if not actual_folder:
                self.log.warning("Image folder not found for %s", book_number)
                skipped_books.append({'book_number': book_number, 'file': docx_file,
                                      'reason': f'Image folder not found in {self.books_dir}'})
                continue

            # Check if images exist
            all_imgs = storage.list_files(actual_folder)
            images = [f for f in all_imgs
                      if f.lower().endswith(('.jpg', '.jpeg', '.png', '.tif', '.tiff'))]
            if not images:
                self.log.warning("No images found in %s", actual_folder)
                skipped_books.append({'book_number': book_number, 'file': docx_file,
                                      'reason': f'No images in folder: {actual_folder}'})
                continue

            self.log.info("Found new book ready for processing: %s", book_number)
            new_books.append({
                'book_number': book_number,
                'docx_file': docx_file,
                'image_folder': actual_folder,
                'num_images': len(images),
                'detected_at': datetime.now().isoformat()
            })
        
        # Log summary of skipped books
        if skipped_books:
            self.log.warning(f"⚠️ {len(skipped_books)} books were skipped:")
            for skip in skipped_books:
                self.log.warning(f"   - {skip.get('book_number', skip.get('file'))}: {skip['reason']}")
            # Store skipped books for debugging
            context['task_instance'].xcom_push(key='skipped_books', value=skipped_books)
        
        if new_books:
            self.log.info(f"🎉 Found {len(new_books)} new books to process!")
            
            # Store new books in XCom for downstream tasks
            context['task_instance'].xcom_push(key='new_books', value=new_books)
            
            return True  # Condition met!
        else:
            # Check for incomplete transfers that need retry
            incomplete_count = self._count_incomplete_transfers()
            if incomplete_count > 0:
                self.log.info(f"🔄 No new books, but found {incomplete_count} incomplete transfers to retry")
                context['task_instance'].xcom_push(key='new_books', value=[])  # Empty list, but proceed
                return True  # Pass to allow retry of incomplete transfers
            
            self.log.info("No new books found and no incomplete transfers")
            return False  # Keep waiting

    @staticmethod
    def _normalize_simple_book_title(title):
        """Return the canonical short title for simple-format books."""
        if not title:
            return ""

        normalized = re.sub(r'\s+', ' ', title).strip()
        normalized = re.sub(
            r'^scheda\s+descrittiva\s+',
            '',
            normalized,
            flags=re.IGNORECASE,
        ).strip()
        return re.split(r'\s*[:–-]\s*', normalized, maxsplit=1)[0].strip()
    
    def _process_simple_format_file(self, docx_file, processed_books,
                                    processed_books_folded=None, prefix=None, storage=None):
        """Handle a simple-format file (NN-<title>.docx).

        Returns:
            dict  – book entry ready to append to new_books
            None  – book already processed (skip silently)
            {'error': str}  – could not process (append to skipped_books)
        """
        from datetime import datetime
        if storage is None:
            storage = get_storage()

        if prefix is None:
            prefix = self._extract_simple_prefix(docx_file)
        if not prefix:
            return {'error': f'Could not extract numeric prefix from: {docx_file}'}

        title = self._normalize_simple_book_title(
            re.sub(r'^\s*\d{2}\s*[^0-9A-Za-z]+\s*', '', docx_file).rsplit('.', 1)[0]
        )

        # Best-effort docx read via storage (download to temp if S3)
        file_path = os.path.join(self.desc_dir, docx_file)
        tmp_path = None
        try:
            from docx import Document
            local_fp = storage.local_path(file_path)
            tmp_path = None if local_fp == file_path else local_fp
            doc = Document(local_fp)
            for para in doc.paragraphs:
                text = para.text.strip()
                if not text:
                    continue
                title_match = re.match(r'^Titolo[:\s]+(.+)$', text, re.IGNORECASE)
                if not title_match:
                    continue
                docx_title = self._normalize_simple_book_title(title_match.group(1).strip())
                if docx_title and docx_title != title:
                    self.log.info(
                        "Simple-format title mismatch for %s: filename=%r docx=%r; using filename",
                        docx_file, title, docx_title,
                    )
                break
        except Exception as exc:
            self.log.warning(
                "Could not read docx title for %s (%s); using filename-derived title %r",
                docx_file, exc, title,
            )
        finally:
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass

        # Use the same key format as extract_simple_format_docx (short_title, not a slug)
        book_number = title  # e.g. "Compactio V"

        # Check if already processed
        book_number_folded = re.sub(r'\s+', ' ', book_number).strip().casefold()
        if processed_books_folded is None:
            processed_books_folded = {
                re.sub(r'\s+', ' ', str(book)).strip().casefold()
                for book in processed_books
                if book
            }
        if (
            book_number in processed_books
            or book_number.upper() in processed_books
            or book_number_folded in processed_books_folded
        ):
            self.log.info(f'Book {book_number} (simple format) already processed, skipping')
            return None

        # Locate the image folder: prefer a normalized title match among directories
        # that start with the numeric prefix.
        image_folder_prefix = f'{prefix}-'
        prefix_re = re.compile(rf'^\s*{re.escape(prefix)}\s*[^0-9A-Za-z]+\s*', re.IGNORECASE)
        candidates = []
        if os.path.exists(self.books_dir):
            for entry in storage.list_dirs(self.books_dir):
                full_path = os.path.join(self.books_dir, entry)
                if entry.startswith(image_folder_prefix) or prefix_re.match(entry):
                    candidates.append(entry)
        self.log.info(
            "Simple-format folder candidates for %r (prefix=%s, title=%r): %s",
            docx_file,
            prefix,
            title,
            candidates,
        )

        actual_folder = None
        if candidates:
            title_folded = re.sub(r'\s+', ' ', title).strip().casefold()
            for entry in candidates:
                entry_title = self._normalize_simple_book_title(
                    re.sub(r'^\s*\d{2}\s*[^0-9A-Za-z]+\s*', '', entry)
                )
                entry_title_folded = re.sub(r'\s+', ' ', entry_title).strip().casefold()
                if entry_title_folded and entry_title_folded == title_folded:
                    actual_folder = os.path.join(self.books_dir, entry)
                    break
            if not actual_folder:
                # Fallback to deterministic order when no exact title match.
                actual_folder = os.path.join(self.books_dir, sorted(candidates)[0])

        if not actual_folder:
            return {
                'error': (
                    f'Image folder not found for simple-format book "{book_number}". '
                    f'Expected a directory starting with "{image_folder_prefix}" in {self.books_dir}'
                )
            }

        all_imgs = storage.list_files(actual_folder)
        images = [
            f for f in all_imgs
            if f.lower().endswith(('.jpg', '.jpeg', '.png', '.tif', '.tiff'))
        ]
        if not images:
            return {'error': f'No images found in {actual_folder}'}

        self.log.info('Found new simple-format book ready for processing: %s (%d images)',
                      book_number, len(images))
        return {
            'book_number': book_number,
            'docx_file': docx_file,
            'image_folder': actual_folder,
            'num_images': len(images),
            'detected_at': datetime.now().isoformat(),
        }

    def _process_generic_format_file(self, docx_file, processed_books_folded, storage):
        """Handle a .docx whose name matches neither the standard nor the simple pattern.

        Uses the filename stem as book_number and searches books_dir for a
        directory whose name (case-insensitive) equals the stem or its slug form.

        Returns:
            dict        – book entry ready to append to new_books
            None        – book already processed (skip silently)
            {'error'}   – could not process (append to skipped_books)
        """
        stem = os.path.splitext(docx_file)[0]
        book_number = stem
        slug = re.sub(r'[^a-z0-9]+', '-', stem.lower()).strip('-')

        book_number_folded = re.sub(r'\s+', ' ', book_number).strip().casefold()
        if book_number_folded in processed_books_folded:
            self.log.info("Book %r (generic format) already processed, skipping", book_number)
            return None

        actual_folder = None
        if storage.exists(self.books_dir):
            for candidate in storage.list_dirs(self.books_dir):
                candidate_lower = candidate.casefold()
                candidate_slug = re.sub(r'[^a-z0-9]+', '-', candidate_lower).strip('-')
                if candidate_lower == book_number_folded or candidate_slug == slug:
                    actual_folder = os.path.join(self.books_dir, candidate)
                    break

        if not actual_folder:
            return {
                'error': (
                    f'No image folder found for "{book_number}". '
                    f'Expected a directory named "{stem}" or "{slug}" in {self.books_dir}'
                )
            }

        all_imgs = storage.list_files(actual_folder)
        images = [
            f for f in all_imgs
            if f.lower().endswith(('.jpg', '.jpeg', '.png', '.tif', '.tiff'))
        ]
        if not images:
            return {'error': f'No images found in {actual_folder}'}

        self.log.info(
            "Found new generic-format book ready for processing: %r (%d images)",
            book_number, len(images),
        )
        return {
            'book_number': book_number,
            'docx_file': docx_file,
            'image_folder': actual_folder,
            'num_images': len(images),
            'detected_at': datetime.now().isoformat(),
        }

    def _count_incomplete_transfers(self):
        """Count books with manifests generated but not yet uploaded to S3."""
        try:
            from operators.processed_books import load_processing_status
            data = load_processing_status()
            incomplete = 0
            for book_number, status in data.get('book_status', {}).items():
                if status.get('manifest_created') and not status.get('manifest_uploaded'):
                    incomplete += 1
            return incomplete
        except Exception:
            return 0

    def _load_processed_books(self):
        """Load set of book numbers that are genuinely fully processed.

        A book is only considered processed if it both appears in the
        `processed_books` list AND has `db_written: true` in `book_status`.
        This prevents books that failed DB writes from being silently skipped.
        """
        try:
            from operators.processed_books import load_processing_status
            data = load_processing_status()

            book_status = data.get('book_status', {})
            genuinely_processed = set()

            for book_number in data.get('processed_books', []):
                status = book_status.get(book_number.upper()) or book_status.get(book_number, {})
                if status.get('db_written', False):
                    genuinely_processed.add(book_number)
                else:
                    self.log.warning(
                        "Book %s is in processed_books but db_written=False — will re-queue",
                        book_number,
                    )

            return genuinely_processed
        except Exception as e:
            self.log.error("Error loading processed books: %s", e)
            return set()
