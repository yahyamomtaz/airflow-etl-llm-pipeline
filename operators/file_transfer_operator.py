from airflow.models import BaseOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
import os

from operators.processed_books import update_book_status
from utils.storage import get_storage


class FileTransferOperator(BaseOperator):
    """
    Transfers files to a remote server using SFTP.
    Supports both single file transfers and batch transfers from XCom.
    
    :param transfer_list_task_id: Task ID to pull transfer list from XCom (for batch mode)
    :param source_path: Path to source file (for single file mode)
    :param dest_path: Path to destination (for single file mode)
    :param ssh_conn_id: The SSH connection id to use
    :param create_intermediate_dirs: Whether to create missing directories on remote
    """
    
    template_fields = ('source_path', 'dest_path')

    def __init__(
        self,
        ssh_conn_id='ssh_default',
        transfer_list_task_id=None,
        source_path=None,
        dest_path=None,
        create_intermediate_dirs=True,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.transfer_list_task_id = transfer_list_task_id
        self.source_path = source_path
        self.dest_path = dest_path
        self.create_intermediate_dirs = create_intermediate_dirs

    def execute(self, context):
        """Execute file transfers using a single SFTP connection"""

        storage = get_storage()
        self.log.info("FileTransfer starting [backend=%s]", storage)

        # Determine transfer mode
        if self.transfer_list_task_id:
            # Batch mode: get transfer list from XCom
            transfer_list = context['ti'].xcom_pull(task_ids=self.transfer_list_task_id)
            if not transfer_list:
                self.log.warning("No transfers to execute")
                return {'success': 0, 'failed': 0, 'results': {}}
        elif self.source_path and self.dest_path:
            # Single file mode
            transfer_list = [{
                'source_path': self.source_path,
                'dest_path': self.dest_path,
                'create_intermediate_dirs': self.create_intermediate_dirs
            }]
        else:
            raise ValueError("Must provide either transfer_list_task_id or both source_path and dest_path")
        
        self.log.info(f"Starting {len(transfer_list)} transfer(s) with single connection...")
        
        # Track results
        success = 0
        failed = 0
        book_results = {}
        
        # Create single SSH/SFTP connection
        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        ssh_client = ssh_hook.get_conn()
        sftp_client = ssh_client.open_sftp()
        
        try:
            for idx, transfer in enumerate(transfer_list):
                source_path = transfer['source_path']
                dest_path = transfer['dest_path']
                
                try:
                    self.log.info("Transfer %d/%d: %s → %s",
                                  idx + 1, len(transfer_list), source_path, dest_path)

                    # Create intermediate directories if needed
                    if transfer.get('create_intermediate_dirs', self.create_intermediate_dirs):
                        dest_dir = os.path.dirname(dest_path)
                        self._ensure_remote_dir(sftp_client, dest_dir)

                    # Transfer the file — works for both local and S3 sources
                    with storage.as_fileobj(source_path) as fobj:
                        sftp_client.putfo(fobj, dest_path)
                    success += 1
                    self.log.info("  Transfer OK")
                    
                    # Track results by book number and file type, and persist to JSON
                    self._track_result(source_path, book_results, success=True,
                                       book_number=transfer.get('book_number'),
                                       file_type=transfer.get('file_type'))

                except Exception as e:
                    failed += 1
                    self.log.error(f"  ❌ Failed: {e}")
                    self._track_result(source_path, book_results, success=False, error=str(e),
                                       book_number=transfer.get('book_number'),
                                       file_type=transfer.get('file_type'))
                    
        finally:
            sftp_client.close()
            ssh_client.close()
        
        self.log.info(f"Transfer Summary: {success} succeeded, {failed} failed")
        
        # Push results to XCom for downstream tasks
        results = {
            'success': success,
            'failed': failed,
            'book_results': book_results
        }
        context['ti'].xcom_push(key='transfer_results', value=results)
        
        return results

    def _ensure_remote_dir(self, sftp_client, remote_dir):
        """Recursively create remote directories if they don't exist"""
        if remote_dir == '/' or remote_dir == '':
            return
        
        try:
            sftp_client.stat(remote_dir)
        except FileNotFoundError:
            parent = os.path.dirname(remote_dir)
            self._ensure_remote_dir(sftp_client, parent)
            try:
                sftp_client.mkdir(remote_dir)
                self.log.info(f"  📁 Created directory: {remote_dir}")
            except IOError:
                pass  # Directory might have been created by another process

    def _track_result(self, source_path, book_results, success, error=None,
                      book_number=None, file_type=None):
        """Track transfer result by book number and persist to processed_books.json"""
        book_num = book_number

        # If the transfer item didn't carry explicit metadata, fall back to
        # deriving them from the path (handles viewer.js and retry transfers).
        if not book_num or not file_type:
            if '/manifests/' in source_path:
                parts = source_path.split('/manifests/')[-1].split('/')
                if parts:
                    book_num = book_num or parts[0].upper()
                    file_type = file_type or 'manifest_copied'
            elif source_path.endswith('-viewer.js'):
                filename = os.path.basename(source_path)
                book_num = book_num or filename.split('-')[0].upper()
                file_type = file_type or 'js_copied'
            elif '/books/' in source_path:
                parts = source_path.split('/books/')[-1].split('/')
                if parts:
                    book_num = book_num or parts[0].upper()
                    file_type = file_type or 'images_copied'

        if book_num and file_type:
            book_results.setdefault(book_num, {})[file_type] = success
            if not success and error:
                book_results[book_num][f'{file_type}_error'] = error

            # Persist to processed_books.json
            update_book_status(book_num, file_type, success, error)


class TransferPrepareOperator(BaseOperator):
    """
    Prepares list of files to transfer for each book.
    
    Pulls books_data from XCom, checks for incomplete transfers from previous runs,
    and creates a transfer list for FileTransferOperator.
    """
    
    template_fields = ('books_dir', 'manifests_dir', 'viewers_dir', 'remote_base')
    
    def __init__(
        self,
        books_dir: str = '/opt/airflow/data/books',
        manifests_dir: str = '/opt/airflow/data/manifests',
        viewers_dir: str = '/opt/airflow/data/viewers',
        remote_base: str = '/remote/storage',  # configure: remote base path for IIIF assets
        website_base: str = '/home/user/website/pages/collections',  # configure: remote website collections path
        extract_task_id: str = 'extract_data',
        **kwargs
    ):
        super().__init__(**kwargs)
        self.books_dir = books_dir
        self.manifests_dir = manifests_dir
        self.viewers_dir = viewers_dir
        self.remote_base = remote_base
        self.website_base = website_base
        self.extract_task_id = extract_task_id
    
    def execute(self, context):
        from operators.processed_books import load_processing_status
        
        self.log.info("Preparing Transfer List")

        books_data = context['task_instance'].xcom_pull(
            key='books_data',
            task_ids=self.extract_task_id
        ) or []

        transfer_list = []
        status_data = load_processing_status()

        # Retry incomplete transfers from previous runs
        self._add_retry_transfers(transfer_list, status_data, books_data)

        # Process current batch
        for data in books_data:
            book_number = data.get('book_number')
            collection_name = data.get('collection_name')
            author_slug = data.get('author_slug')

            if not all([book_number, collection_name, author_slug]):
                self.log.warning("Skipping book due to missing fields: %s", list(data.keys()))
                continue

            self._add_book_transfers(
                transfer_list,
                book_number,
                collection_name,
                author_slug,
                status_data
            )

        self.log.info("Prepared %d file transfer(s)", len(transfer_list))
        return transfer_list
    
    def _add_retry_transfers(self, transfer_list, status_data, books_data):
        """Add incomplete transfers from previous runs"""
        for book_number, status in status_data.get('book_status', {}).items():
            # Skip if this book is in current batch
            if any(d.get('book_number', '').upper() == book_number.upper() for d in books_data):
                continue
            
            # Retry incomplete JS copy
            if status.get('js_created') and not status.get('js_copied'):
                js_files = [f for f in os.listdir(self.viewers_dir) 
                           if f.startswith(book_number.lower()) and f.endswith('-viewer.js')]
                if js_files:
                    js_file = js_files[0]
                    local_viewer = os.path.join(self.viewers_dir, js_file)
                    collection = 'cinquecentine'
                    website_dir = f'{self.website_base}/{collection}'
                    transfer_list.append({
                        'source_path': local_viewer,
                        'dest_path': os.path.join(website_dir, js_file),
                        'create_intermediate_dirs': True
                    })
                    self.log.info(f"   🔄 Retrying incomplete JS transfer for {book_number}")
            
            # Retry incomplete manifest copy
            if status.get('manifest_created') and not status.get('manifest_copied'):
                local_manifest = os.path.join(self.manifests_dir, book_number.lower(), 'manifest.json')
                if os.path.exists(local_manifest):
                    js_files = [f for f in os.listdir(self.viewers_dir) if f.startswith(book_number.lower())]
                    if js_files:
                        parts = js_files[0].replace('-viewer.js', '').split('-', 1)
                        author_slug = parts[1] if len(parts) > 1 else 'unknown'
                        collection = 'cinquecentine'
                        remote_book_dir = os.path.join(self.remote_base, collection, f"{book_number.lower()}-{author_slug}")
                        transfer_list.append({
                            'source_path': local_manifest,
                            'dest_path': os.path.join(remote_book_dir, 'manifest.json'),
                            'create_intermediate_dirs': True
                        })
                        self.log.info(f"   🔄 Retrying incomplete manifest transfer for {book_number}")
    
    def _add_book_transfers(self, transfer_list, book_number, collection_name, author_slug, status_data):
        """Add transfers for a single book"""
        remote_book_dir = os.path.join(
            self.remote_base, 
            collection_name.lower(), 
            f"{book_number.lower()}-{author_slug.lower()}"
        )
        
        local_images_dir = os.path.join(self.books_dir, book_number.lower())
        local_manifest = os.path.join(self.manifests_dir, book_number.lower(), "manifest.json")
        viewer_filename = f"{book_number.lower()}-{author_slug.lower()}-viewer.js"
        local_viewer = os.path.join(self.viewers_dir, viewer_filename)
        
        book_status = status_data.get('book_status', {}).get(book_number.upper(), {})
        
        # Manifest
        if not book_status.get('manifest_copied', False):
            transfer_list.append({
                'source_path': local_manifest,
                'dest_path': os.path.join(remote_book_dir, 'manifest.json'),
                'create_intermediate_dirs': True
            })

        # Images
        if not book_status.get('images_copied', False) and os.path.exists(local_images_dir):
            for filename in os.listdir(local_images_dir):
                full_path = os.path.join(local_images_dir, filename)
                if os.path.isfile(full_path):
                    transfer_list.append({
                        'source_path': full_path,
                        'dest_path': os.path.join(remote_book_dir, filename),
                        'create_intermediate_dirs': True
                    })
        
        # JS Viewer
        website_collections_dir = f'{self.website_base}/{collection_name.lower()}'
        if not book_status.get('js_copied', False) and os.path.exists(local_viewer):
            transfer_list.append({
                'source_path': local_viewer,
                'dest_path': os.path.join(website_collections_dir, viewer_filename),
                'create_intermediate_dirs': True
            })
            self.log.info(f"   📦 Added JS transfer: {viewer_filename} -> {website_collections_dir}")

