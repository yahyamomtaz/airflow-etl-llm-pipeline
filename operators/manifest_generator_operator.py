"""
IIIF Manifest Generator Operator
Generates IIIF manifests for books, can be used as a function or Airflow operator
"""

import os
import json
import logging
from airflow.models import BaseOperator
from utils.storage import get_storage

log = logging.getLogger(__name__)

def generate_manifest(data, image_dir, output_dir, storage=None):
    """Generate IIIF manifest.json for a book.

    Args:
        data:       Book metadata dictionary
        image_dir:  Directory containing the book's images
        output_dir: Directory where the manifest should be saved
        storage:    Storage backend (LocalStorage or S3Storage); resolved
                    automatically from Airflow Variable if not supplied.
    """
    if storage is None:
        storage = get_storage()

    book_label = data.get('book_number', 'unknown')
    log.info("Generating manifest for %s", book_label)

    # List image files via storage backend (works on local fs or S3)
    all_files = storage.list_files(image_dir)
    image_files = sorted(f for f in all_files if f.lower().endswith('.jpg'))

    if not image_files:
        log.warning("No images found in %s", image_dir)
        return None

    log.info("Found %d image(s) for %s", len(image_files), book_label)
    
    # Build the slug used in URLs.
    # Simple-format books provide an explicit 'book_slug' (e.g. "compactio-i").
    # Standard books derive it from book_number + author_slug as before.
    author_slug = data.get('author_slug', '')
    if not author_slug or author_slug == 'unknown-author':
        author_slug = ''
        data['author_slug'] = ''

    if data.get('book_slug'):
        # Simple-format: slug is pre-computed (e.g. "compactio-i")
        book_slug = data['book_slug']
    elif author_slug:
        book_slug = f"{data['book_number'].lower()}-{author_slug.lower()}"
    else:
        book_slug = data['book_number'].lower()

    try:
        from airflow.models import Variable
        base_url = Variable.get(
            'IIIF_BASE_URL',
            default_var=os.getenv('IIIF_BASE_URL', 'https://your-iiif-server.example.com/collections'),
        ).rstrip('/')
    except Exception:
        base_url = os.getenv('IIIF_BASE_URL', 'https://your-iiif-server.example.com/collections').rstrip('/')
    image_url_prefix = f"{base_url}/{data['collection_name'].lower()}/{book_slug}"

    # Generate IIIF manifest
    manifest = {
        "@context": "http://iiif.io/api/presentation/2/context.json",
        "@id": f"{image_url_prefix}/manifest.json",
        "@type": "sc:Manifest",
        "label": book_slug,
        "metadata": [
            {
                "label": "Author",
                "value": author_slug.lower() if author_slug else ""
            },
            {
                "label": "Book ID",
                "value": data['book_number']
            }
        ],
        "sequences": [
            {
                "@type": "sc:Sequence",
                "canvases": []
            }
        ]
    }

    # Generate canvases for each image
    for index, file_name in enumerate(image_files):
        canvas_id = f"{image_url_prefix}/canvas{index + 1}"
        
        # Extract label from filename
        label = file_name.split('_', 1)[-1].rsplit('.', 1)[0] if '_' in file_name else str(index + 1)

        canvas = {
            "@id": canvas_id,
            "@type": "sc:Canvas",
            "label": label,
            "height": 3933,
            "width": 2645,
            "images": [
                {
                    "@type": "oa:Annotation",
                    "motivation": "sc:painting",
                    "resource": {
                        "@id": f"{image_url_prefix}/{file_name}",
                        "@type": "dctypes:Image",
                        "format": "image/jpeg",
                        "height": 3933,
                        "width": 2645
                    },
                    "on": canvas_id
                }
            ]
        }
        manifest["sequences"][0]["canvases"].append(canvas)

    # Write manifest.json via storage backend (local file or S3 object)
    storage.makedirs(output_dir)
    manifest_path = os.path.join(output_dir, "manifest.json")
    storage.write_text(
        manifest_path,
        json.dumps(manifest, indent=4, ensure_ascii=False),
    )
    log.info("Manifest written: %s", manifest_path)
    return manifest_path


class ManifestGeneratorOperator(BaseOperator):
    """
    Airflow operator that generates IIIF manifests for all books in XCom.
    
    Pulls books_data from XCom, generates manifests, and updates book status.
    """
    
    template_fields = ('books_dir', 'manifests_dir')
    
    def __init__(
        self,
        books_dir: str = '/opt/airflow/data/books',
        manifests_dir: str = '/opt/airflow/data/manifests',
        extract_task_id: str = 'extract_data',
        **kwargs
    ):
        super().__init__(**kwargs)
        self.books_dir = books_dir
        self.manifests_dir = manifests_dir
        self.extract_task_id = extract_task_id
    
    def execute(self, context):
        from operators.processed_books import update_book_status

        storage = get_storage()
        self.log.info("Generating IIIF Manifests [backend=%s]", storage)

        # Pull books data from XCom
        books_data = context['task_instance'].xcom_pull(
            key='books_data',
            task_ids=self.extract_task_id
        )

        if not books_data:
            self.log.warning("No books data found in XCom")
            return 0

        storage.makedirs(self.manifests_dir)

        success = 0
        for data in books_data:
            book_number = data.get('book_number', 'Unknown')
            # Use explicit image_folder_name if provided (simple-format books),
            # otherwise fall back to book_number (standard books).
            folder_name = data.get('image_folder_name') or book_number.lower()
            image_dir = os.path.join(self.books_dir, folder_name)

            if not os.path.exists(image_dir):
                self.log.warning("Image directory not found: %s", image_dir)
                update_book_status(book_number, 'manifest_created', False, f'Image directory not found: {image_dir}')
                continue

            book_slug_for_dir = data.get('book_slug') or book_number.lower()
            manifest_output_dir = os.path.join(self.manifests_dir, book_slug_for_dir)

            try:
                if generate_manifest(data, image_dir, manifest_output_dir, storage=storage):
                    success += 1
                    update_book_status(book_number, 'manifest_created', True)
                else:
                    update_book_status(book_number, 'manifest_created', False, 'generate_manifest returned None')
            except Exception as e:
                self.log.error("Error generating manifest for %s: %s", book_number, e)
                update_book_status(book_number, 'manifest_created', False, str(e))

        self.log.info("Generated %d/%d manifests", success, len(books_data))
        return success
