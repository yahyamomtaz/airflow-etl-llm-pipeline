import os
import json
import re
import sqlite3

def strip_html_tags(text):
    """Remove HTML tags from text while preserving the content"""
    if not text:
        return text
    clean_text = re.sub(r'<[^>]+>', '', text)
    return clean_text.strip()

def get_author_from_database(book_id, db_path="books/collections.db"):
    """Get author name from database based on book ID (number)"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Try to get author from book_descriptions first (has HTML)
        cursor.execute("""
            SELECT author FROM book_descriptions 
            WHERE number = ? AND language = 'it'
            LIMIT 1
        """, (book_id.upper(),))
        
        result = cursor.fetchone()
        if result and result[0]:
            author = result[0]
            # Strip HTML tags
            author = strip_html_tags(author)
            # Clean up extra info (dates, etc.)
            author = re.split(r'[<\(]', author)[0].strip()
            author = author.rstrip('.,;')
            conn.close()
            return author if author else "Unknown Author"
        
        # Fallback: try books table
        cursor.execute("""
            SELECT author FROM books 
            WHERE number = ?
            LIMIT 1
        """, (book_id.upper(),))
        
        result = cursor.fetchone()
        conn.close()
        
        if result and result[0]:
            author = result[0]
            author = strip_html_tags(author)
            author = re.split(r'[<\(]', author)[0].strip()
            author = author.rstrip('.,;')
            return author if author else "Unknown Author"
        
        return "Unknown Author"
        
    except Exception as e:
        print(f"   ⚠️  Error getting author from database: {e}")
        return "Unknown Author"

def process_book_folder(book_folder):
    """Process a book folder: get author from DB, generate manifest and JS viewer"""
    print(f"⚙️ Processing {book_folder}...")

    # Get book ID (folder name)
    book_id = os.path.basename(book_folder)

    # Get author from database
    author = get_author_from_database(book_id)
    print(f"   📝 Author: {author}")

    # Get image files
    image_files = sorted([f for f in os.listdir(book_folder) if f.endswith('.jpg')])
    
    if not image_files:
        print(f"   ⚠️  No images found in {book_folder}")
        return
    
    print(f"   🖼️  Found {len(image_files)} images")
    
    # Create URL-safe author slug
    author_slug = re.sub(r'[^a-z0-9]+', '-', author.lower()).strip('-')
    if not author_slug or author_slug == 'unknown-author':
        author_slug = "unknown"
    
    iiif_base = os.getenv('IIIF_BASE_URL', 'https://your-iiif-server.example.com/collections').rstrip('/')
    image_url_prefix = f"{iiif_base}/collection-a/{book_id.lower()}-{author_slug}"

    # Generate IIIF manifest
    manifest = {
        "@context": "http://iiif.io/api/presentation/2/context.json",
        "@id": f"{image_url_prefix}/manifest.json",
        "@type": "sc:Manifest",
        "label": f"{book_id} - {author}",
        "metadata": [
            {
                "label": "Author",
                "value": author
            },
            {
                "label": "Book ID",
                "value": book_id
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

    # Write manifest.json
    manifest_path = os.path.join(book_folder, "manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=4, ensure_ascii=False)

    print(f"   ✅ Manifest generated: {manifest_path}")

    # Generate JS viewer file
    generate_js_file(book_folder, book_id, author, image_url_prefix)
    
    print(f"   ✅ Processing complete for {book_id}")

def generate_js_file(folder, book_id, author, manifest_url):
    """Generate JavaScript viewer file with clean author name using correct template"""
    
    # Create safe component name (PascalCase)
    import re
    def make_valid_identifier(name):
        # Replace hyphens and invalid characters with underscores
        name = re.sub(r'\W|^(?=\d)', '_', name)
        # Capitalize parts to make it PascalCase
        parts = name.split('_')
        return ''.join(part.capitalize() for part in parts if part)
    
    component_name = 'Viewer' + make_valid_identifier(book_id)
    js_filename = f"{book_id.lower()}-viewer.js"
    js_path = os.path.join(folder, js_filename)

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
          id: 'mirador-viewer-{book_id.lower()}',
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

    with open(js_path, "w", encoding="utf-8") as f:
        f.write(js_content)

    print(f"   🧩 Viewer JS created: {js_path}")
    print(f"      Component: {component_name}")
    print(f"      Author: {author}")