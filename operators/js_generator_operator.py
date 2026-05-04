"""
JS Viewer Generator Operator
Generates React/Next.js viewer components for books, can be used as a function or Airflow operator
"""

import os
import re
import logging
from airflow.models import BaseOperator
from .data_extractor import strip_html_tags

log = logging.getLogger(__name__)

def generate_js_viewer(data, output_dir):
    """Generate React viewer component"""
    log.info(f"\n🧩 Generating JS viewer for {data['book_number']}...")
    
    # Strip HTML tags from author
    author_clean = strip_html_tags(data['author'])
    
    # Create safe component name (PascalCase)
    def make_valid_identifier(name):
        name = re.sub(r'\W|^(?=\d)', '_', name)
        parts = name.split('_')
        return ''.join(part.capitalize() for part in parts if part)
    
    component_name = 'Viewer' + make_valid_identifier(data['book_number'])
    author_slug = re.sub(r'[^a-z0-9]+', '-', author_clean.lower()).strip('-')
    js_filename = f"{data['book_number'].lower()}-{author_slug.lower()}-viewer.js"
    js_path = os.path.join(output_dir, js_filename)
    iiif_base = os.getenv('IIIF_BASE_URL', 'https://your-iiif-server.example.com/collections').rstrip('/')
    manifest_url = f"{iiif_base}/{data['collection_name'].lower()}/{data['book_number'].lower()}-{author_slug.lower()}"

    js_content = f"""'use client';
import React from 'react';
import dynamic from 'next/dynamic';

const MiradorViewer = dynamic(
  () => import('../../../components/MiradorWrapper'),
  {{ ssr: false }}
);

function {component_name}() {{
  return (
    <div className="viewer-container" style={{{{
      height: '100vh',
      width: '100%',
      margin: 0,
      padding: 0,
      overflow: 'hidden',
      position: 'relative',
      display: 'flex',
      flexDirection: 'column'
    }}}}>
      <style jsx global>{{`
        html, body {{
          margin: 0;
          padding: 0;
          height: 100%;
          overflow: hidden;
        }}

        #__next, main {{
          height: 100%;
          margin: 0;
          padding: 0;
        }}
      `}}</style>

      <MiradorViewer 
        config={{{{
          id: 'mirador-viewer-{data['book_number'].lower()}-{author_slug.lower()}',
          selectedTheme: 'dark',
          themes: {{
            dark: {{
              palette: {{
                mode: 'dark',
                primary: {{ main: '#262426' }},
                secondary: {{ main: '#d9b991' }}
              }}
            }}
          }},
          windows: [
            {{
              loadedManifest: '{manifest_url}/manifest.json',
              canvasIndex: 0
            }}
          ],
          window: {{
            allowClose: false,
            allowMaximize: false,
            allowFullscreen: true,
            allowWindowSideBar: true,
            sideBarOpenByDefault: false
          }},
          workspace: {{
            showZoomControls: true,
            type: 'mosaic'
          }},
          thumbnailNavigation: {{
            defaultPosition: 'far-bottom',
            displaySettings: true
          }}
        }}}}
      />
    </div>
  );
}}

export default {component_name};
"""

    os.makedirs(output_dir, exist_ok=True)
    
    with open(js_path, "w", encoding="utf-8") as f:
        f.write(js_content)

    log.info(f"   ✅ Viewer JS created: {js_path}")
    log.info(f"      Component: {component_name}")
    log.info(f"      Author: {author_clean}")
    return js_path


class JSViewerGeneratorOperator(BaseOperator):
    """
    Airflow operator that generates JS viewers for all books in XCom.
    
    Pulls books_data from XCom, generates JS viewers, and updates book status.
    """
    
    template_fields = ('viewers_dir',)
    
    def __init__(
        self,
        viewers_dir: str = '/opt/airflow/data/viewers',
        extract_task_id: str = 'extract_data',
        **kwargs
    ):
        super().__init__(**kwargs)
        self.viewers_dir = viewers_dir
        self.extract_task_id = extract_task_id
    
    def execute(self, context):
        from operators.processed_books import update_book_status
        
        self.log.info("=" * 70)
        self.log.info("Generating JS Viewers")
        self.log.info("=" * 70)
        
        # Pull books data from XCom
        books_data = context['task_instance'].xcom_pull(
            key='books_data', 
            task_ids=self.extract_task_id
        )
        
        if not books_data:
            self.log.warning("⚠️  No books data found in XCom")
            return 0
        
        os.makedirs(self.viewers_dir, exist_ok=True)
        
        success = 0
        for data in books_data:
            book_number = data.get('book_number', 'Unknown')
            
            try:
                if generate_js_viewer(data, self.viewers_dir):
                    success += 1
                    update_book_status(book_number, 'js_created', True)
                else:
                    update_book_status(book_number, 'js_created', False, 'generate_js_viewer returned None')
            except Exception as e:
                self.log.error(f"❌ Error generating JS viewer for {book_number}: {e}")
                update_book_status(book_number, 'js_created', False, str(e))
        
        self.log.info(f"\n✅ Generated {success}/{len(books_data)} JS viewers")
        return success
