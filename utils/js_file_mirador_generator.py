import paramiko
import os
import re

#SFTP_HOST = os.getenv('SSH_HOST')
#SFTP_PORT = int(os.getenv('SSH_PORT', '22'))
#SFTP_USER = os.getenv('SSH_USER')
#SFTP_PASS = os.getenv('SSH_PASSWORD')

BOOK_ROOT = 'books/illuminated-dante-project'
OUTPUT_DIR = 'generated_viewers_new'

TEMPLATE = '''\
'use client';
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
      <style jsx global>{{`\
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
        }}}}
      `}}</style>

      <MiradorViewer 
        config={{{{
          id: 'mirador-viewer-{book_folder}',
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
              loadedManifest: `${{process.env.NEXT_PUBLIC_IIIF_BASE_URL}}/collection-a/{book_folder}/manifest.json`,
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
'''

def make_valid_identifier(name):
    # Replace hyphens and invalid characters with underscores
    name = re.sub(r'\W|^(?=\d)', '_', name)
    # Capitalize parts to make it PascalCase
    parts = name.split('_')
    return ''.join(part.capitalize() for part in parts if part)
def stat_isdir(path):
    import stat
    try:
        return stat.S_ISDIR(os.stat(path).st_mode)
    except IOError:
        return False

def main():
    # Connect to SFTP
    #transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    #transport.connect(username=SFTP_USER, password=SFTP_PASS)
    #sftp = paramiko.SFTPClient.from_transport(transport)

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    for book_folder in os.listdir(BOOK_ROOT):
        book_path = f"{BOOK_ROOT}/{book_folder}"
        if not stat_isdir(book_path):
            continue

        js_filename = f"{book_folder}-viewer.js"
        component_name = 'Viewer' + make_valid_identifier(book_folder)
        js_content = TEMPLATE.format(
            component_name=component_name,
            book_folder=book_folder
        )
        js_path = os.path.join(OUTPUT_DIR, js_filename)
        with open(js_path, 'w') as f:
            f.write(js_content)
        print(f"Generated {js_path}")

if __name__ == "__main__":
    main()
