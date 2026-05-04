import os
import sys
import re
import logging
import psycopg2
import psycopg2.extras
from .data_extractor import extract_data_from_docx

log = logging.getLogger(__name__)

# ===== Credentials will be loaded dynamically =====

DESC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "descriptions"))


# ─── helpers ────────────────────────────────────────────────────────────────

def _require(name, value):
    if value:
        return value
    raise RuntimeError(f"Missing required credential: {name}")


def _load_credentials():
    """Load DB and SSH credentials from Airflow Connections, falling back to env vars."""
    try:
        from airflow.hooks.base import BaseHook
        from airflow.models import Variable
        pg  = BaseHook.get_connection('pipeline_postgres')
        ssh = BaseHook.get_connection('pipeline_ssh')
        return {
            'db_host':     pg.host or Variable.get('PIPELINE_DB_HOST', default_var=None),
            'db_port':     pg.port or int(Variable.get('PIPELINE_DB_PORT', default_var='5432')),
            'db_name':     pg.schema or Variable.get('PIPELINE_DB_NAME', default_var=None),
            'db_user':     pg.login or Variable.get('PIPELINE_DB_USER', default_var=None),
            'db_password': pg.get_password() or Variable.get('PIPELINE_DB_PASSWORD', default_var=None),
            'ssh_host':    ssh.host or Variable.get('SSH_HOST', default_var=None),
            'ssh_port':    ssh.port or int(Variable.get('SSH_PORT', default_var='22')),
            'ssh_user':    ssh.login or Variable.get('SSH_USER', default_var=None),
            'ssh_password': ssh.get_password() or Variable.get('SSH_PASSWORD', default_var=None),
        }
    except Exception:
        # Running outside Airflow (CLI mode) — use env vars
        return {
            'db_host':     os.getenv('PIPELINE_DB_HOST'),
            'db_port':     int(os.getenv('PIPELINE_DB_PORT', '5432')),
            'db_name':     os.getenv('PIPELINE_DB_NAME'),
            'db_user':     os.getenv('PIPELINE_DB_USER'),
            'db_password': os.getenv('PIPELINE_DB_PASSWORD'),
            'ssh_host':    os.getenv('SSH_HOST'),
            'ssh_port':    int(os.getenv('SSH_PORT', '22')),
            'ssh_user':    os.getenv('SSH_USER'),
            'ssh_password': os.getenv('SSH_PASSWORD'),
        }


def _get_conn():
    """
    Return a new psycopg2 connection to the PostgreSQL database via SSH tunnel.

    PostgreSQL on the remote server only listens on ::1 (IPv6 loopback), so we
    open a local port-forward through SSH using paramiko and connect through it.
    """
    import socket
    import threading
    import paramiko

    creds = _load_credentials()

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        _require("ssh_host", creds['ssh_host']),
        port=creds['ssh_port'],
        username=_require("ssh_user", creds['ssh_user']),
        password=_require("ssh_password", creds['ssh_password']),
        timeout=15,
    )

    local_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    local_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    local_sock.bind(('127.0.0.1', 0))
    local_port = local_sock.getsockname()[1]
    local_sock.listen(1)

    db_port = creds['db_port']

    def _forward():
        try:
            client, _ = local_sock.accept()
            transport = ssh.get_transport()
            channel = transport.open_channel(
                'direct-tcpip',
                ('::1', db_port),
                ('127.0.0.1', local_port),
            )
            while True:
                import select
                r, _, _ = select.select([client, channel], [], [], 1)
                if client in r:
                    data = client.recv(4096)
                    if not data:
                        break
                    channel.send(data)
                if channel in r:
                    data = channel.recv(4096)
                    if not data:
                        break
                    client.send(data)
        except Exception:
            pass
        finally:
            try:
                client.close()
            except Exception:
                pass
            try:
                channel.close()
            except Exception:
                pass

    t = threading.Thread(target=_forward, daemon=True)
    t.start()

    conn = psycopg2.connect(
        host='127.0.0.1',
        port=local_port,
        dbname=_require("db_name", creds['db_name']),
        user=_require("db_user", creds['db_user']),
        password=_require("db_password", creds['db_password']),
        connect_timeout=15,
    )

    class _TunneledConnection:
        def __init__(self, c, sock, ssh_client):
            self._c = c
            self._sock = sock
            self._ssh_client = ssh_client

        def __getattr__(self, name):
            return getattr(self._c, name)

        def close(self):
            try:
                self._c.close()
            finally:
                try:
                    self._sock.close()
                except Exception:
                    pass
                try:
                    self._ssh_client.close()
                except Exception:
                    pass

    return _TunneledConnection(conn, local_sock, ssh)


def normalize_book_number(number):
    """Normalize book number to handle different formats like 5A01 vs 5A1."""
    if not number:
        return ""
    number = str(number).strip().upper()
    match = re.match(r'^(\d+)([A-Z]+)(\d+)$', number)
    if match:
        prefix, letter, num = match.groups()
        return f"{prefix}{letter}{int(num)}"
    return number


def strip_html_tags(text):
    """Strip HTML tags from text, keeping only the visible content."""
    if not text:
        return text
    if not isinstance(text, str):
        return text
    return re.sub(r'<[^>]+>', '', str(text)).strip()


# ─── core database logic ─────────────────────────────────────────────────────

def find_or_create_book(cursor, data):
    """Find a book in the specified collection, or create it if not found."""
    normalized_book_number = normalize_book_number(data["book_number"])

    cursor.execute(
        "SELECT book_id, number FROM books WHERE collection_id = %s AND UPPER(number) = %s",
        (data["collection_id"], normalized_book_number.upper())
    )
    log.info("Searching for book: %s in collection %s", normalized_book_number, data['collection_id'])
    result = cursor.fetchone()
    if result:
        return result, "found"

    cursor.execute(
        "SELECT book_id, number FROM books WHERE collection_id = %s",
        (data["collection_id"],)
    )
    for book_id, db_number in cursor.fetchall():
        if normalize_book_number(db_number) == normalized_book_number:
            return (book_id, db_number), "found"

    log.info("Creating new book: %s in database", normalized_book_number)
    try:
        books_title = (
            data["book_number"]
            if data.get('image_folder_name')
            else strip_html_tags(data["title"])
        )
        insert_data = {
            'collection_id':   data["collection_id"],
            'collection_name': data["collection_name"],
            'title':           books_title,
            'digital_source':  data.get('book_slug') or f'{data["book_number"].lower()}-{data["author_slug"]}',
            'number':          normalized_book_number,
            'author':          strip_html_tags(data["author"]) or 'Unknown',
        }
        columns      = list(insert_data.keys())
        placeholders = ["%s"] * len(columns)
        sql = (
            f"INSERT INTO books ({', '.join(columns)}) "
            f"VALUES ({', '.join(placeholders)}) RETURNING book_id"
        )
        cursor.execute(sql, list(insert_data.values()))
        book_id = cursor.fetchone()[0]
        return (book_id, data["book_number"]), "created"
    except Exception as e:
        return None, f"error: {str(e)}"


def insert_or_update_description(cursor, book_id, collection_id, collection_name, book_number, data):
    """Insert or update a book description in the book_descriptions table."""

    cursor.execute(
        "SELECT description_id FROM book_descriptions WHERE book_id = %s AND language = 'it'",
        (book_id,)
    )
    existing = cursor.fetchone()

    _BOOKS_TABLE_FIELDS = {
        'book_number', 'collection_id', 'collection_name',
        'author_slug', 'digital_source', 'book_slug', 'image_folder_name',
    }

    full_data = {
        'book_id':         book_id,
        'collection_id':   collection_id,
        'number':          book_number,
        'language':        'it',
        **{k: v for k, v in data.items() if k not in _BOOKS_TABLE_FIELDS},
    }

    if existing:
        description_id = existing[0]
        set_clauses, values = [], []
        for key, value in data.items():
            if key not in _BOOKS_TABLE_FIELDS and value:
                set_clauses.append(f"{key} = %s")
                values.append(str(value))
        if set_clauses:
            values.append(description_id)
            sql = f"UPDATE book_descriptions SET {', '.join(set_clauses)} WHERE description_id = %s"
            cursor.execute(sql, values)
            return "updated"
        return "no_changes"
    else:
        clean_data   = {k: str(v) if v else None for k, v in full_data.items()}
        columns      = list(clean_data.keys())
        placeholders = ["%s"] * len(columns)
        sql = f"INSERT INTO book_descriptions ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        cursor.execute(sql, list(clean_data.values()))
        return "inserted"


def fetch_collection_slug(cursor, collection_id):
    """Look up the slug for a collection from the collections table."""
    cursor.execute(
        "SELECT slug FROM collections WHERE id = %s",
        (collection_id,)
    )
    row = cursor.fetchone()
    return row[0] if row else None


def upsert_viewer(cursor, book_id, data, db_number):
    """Insert or update a row in the `viewers` table for this book."""
    collection_id   = data.get("collection_id")
    collection_name = data.get("collection_name", "").lower()
    author_slug     = data.get("author_slug", "").lower()
    title           = strip_html_tags(data.get("title", ""))

    if data.get('book_slug'):
        slug = data['book_slug']
    elif author_slug:
        slug = f"{db_number.lower()}-{author_slug}"
    else:
        slug = db_number.lower()

    collection_slug = fetch_collection_slug(cursor, collection_id)
    if not collection_slug:
        collection_slug = f"{collection_name}-accademia-pontaniana"
        log.warning("Could not fetch collection slug for id=%s, using fallback: %s",
                    collection_id, collection_slug)

    manifest_url = (
        f"{os.getenv('IIIF_BASE_URL', 'https://your-iiif-server.example.com/collections')}/{collection_name}"
        f"/{slug}/manifest.json"
    )

    sql = """
        INSERT INTO viewers
            (slug, title, collection_slug, manifest_url, collection_id,
             is_active, theme, canvas_index, created_at, updated_at)
        VALUES
            (%s, %s, %s, %s, %s,
             true, 'dark', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (slug) DO UPDATE SET
            title           = EXCLUDED.title,
            collection_slug = EXCLUDED.collection_slug,
            manifest_url    = EXCLUDED.manifest_url,
            collection_id   = EXCLUDED.collection_id,
            updated_at      = CURRENT_TIMESTAMP
        RETURNING id, (xmax = 0) AS inserted
    """
    cursor.execute(sql, (slug, title, collection_slug, manifest_url, collection_id))
    row = cursor.fetchone()
    viewer_id, was_inserted = row
    action = "inserted" if was_inserted else "updated"
    log.info("Viewer %s: id=%s slug='%s' manifest_url=%s", action, viewer_id, slug, manifest_url)
    return action


def update_db_from_docx(data):
    """Update PostgreSQL database with extracted book data."""

    book_number     = data.get("book_number")
    collection_id   = data.get("collection_id")
    collection_name = data.get("collection_name")

    if not collection_id or not book_number:
        log.error("Missing collection_id or book_number in data")
        return False

    log.info("Processing book: %s  collection: %s (ID: %s)", book_number, collection_name, collection_id)

    try:
        conn   = _get_conn()
        cursor = conn.cursor()
    except Exception as e:
        log.error("Could not connect to PostgreSQL: %s", e)
        return False

    try:
        book_result = find_or_create_book(cursor, data)

        if book_result[1].startswith("error"):
            log.error("Error finding/creating book: %s", book_result[1])
            conn.rollback()
            return False

        book_match, status = book_result
        if book_match is None:
            log.error("Could not find or create book")
            conn.rollback()
            return False

        book_id, db_number = book_match
        log.info("%s book: %s (ID: %s)", "Created" if status == 'created' else "Found", db_number, book_id)

        digital_source = (
            data.get('book_slug')
            or f'{data["book_number"].lower()}-{data.get("author_slug", "")}'.strip('-')
        )
        books_title = (
            data["book_number"]
            if data.get('image_folder_name')
            else strip_html_tags(data["title"])
        )
        cursor.execute(
            "UPDATE books SET digital_source = %s, title = %s WHERE book_id = %s",
            (digital_source, books_title, book_id),
        )
        log.info("digital_source='%s', title='%s'", digital_source, books_title)

        action = insert_or_update_description(cursor, book_id, collection_id, collection_name, db_number, data)
        log.info("Description %s for book %s", action, book_number)

        upsert_viewer(cursor, book_id, data, db_number)

        for col, value in data.items():
            if value and '<a href=' in str(value):
                log.info("Field '%s' has hyperlinks: %s...", col, str(value)[:100])

        conn.commit()
        log.info("Database updated successfully for book %s", book_number)
        return True

    except Exception as e:
        log.error("Database error for book %s: %s", book_number, e)
        import traceback
        traceback.print_exc()
        conn.rollback()
        return False
    finally:
        conn.close()


# ─── Airflow Operator ────────────────────────────────────────────────────────

from airflow.models import BaseOperator


class DatabaseUpdateOperator(BaseOperator):
    """
    Airflow operator that writes book data to the remote PostgreSQL database
    via an SSH tunnel (PostgreSQL only listens on localhost on the server).
    """

    template_fields = ()

    def __init__(
        self,
        extract_task_id: str = 'extract_data',
        **kwargs
    ):
        super().__init__(**kwargs)
        self.extract_task_id = extract_task_id

    def execute(self, context):
        creds = _load_credentials()
        self.log.info("Updating PostgreSQL Database")
        self.log.info("DB: %s@%s:%s/%s  (via SSH tunnel)",
                      creds['db_user'], creds['ssh_host'], creds['db_port'], creds['db_name'])

        books_data = context['task_instance'].xcom_pull(
            key='books_data',
            task_ids=self.extract_task_id
        )

        if not books_data:
            self.log.warning("No books data found in XCom")
            return 0

        success = 0
        for data in books_data:
            if update_db_from_docx(data):
                success += 1

        self.log.info("Updated %d/%d books in PostgreSQL", success, len(books_data))
        return success


# ─── CLI entry point ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    log.info("Database Update from Word Documents -> PostgreSQL")

    docx_files = [f for f in os.listdir(DESC_DIR) if f.lower().endswith('.docx')]

    if not docx_files:
        log.error("No .docx files found in %s", DESC_DIR)
        sys.exit(1)

    log.info("Found %d Word document(s)", len(docx_files))

    success_count = 0
    error_count   = 0

    for filename in docx_files:
        file_path = os.path.join(DESC_DIR, filename)
        log.info("Processing: %s", filename)
        try:
            data = extract_data_from_docx(file_path)
            if not data:
                log.warning("No data extracted from %s", filename)
                error_count += 1
                continue
            if update_db_from_docx(data):
                success_count += 1
            else:
                error_count += 1
        except Exception as e:
            log.error("Error processing %s: %s", filename, e)
            import traceback
            traceback.print_exc()
            error_count += 1

    log.info("Summary: successfully processed=%d errors=%d", success_count, error_count)
