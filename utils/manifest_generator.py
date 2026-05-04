import os
import json

# Path to the images directory
image_directory = 'books/<your-book-folder>'  # configure: path to your image directory
# URL prefix for the images — set IIIF_BASE_URL env var or replace the placeholder below
image_url_prefix = f"{os.getenv('IIIF_BASE_URL', 'https://your-iiif-server.example.com/collections')}/collection-a/<your-book-slug>"

image_files = sorted([f for f in os.listdir(image_directory) if f.endswith('.png')])

manifest = {
    "@context": "http://iiif.io/api/presentation/2/context.json",
    "@id": f"{image_url_prefix}/manifest.json",
    "@type": "sc:Manifest",
    "label": "venezia-marciana-it-z-54",
    "sequences": [
        {
            "@type": "sc:Sequence",
            "canvases": []
        }
    ]
}

def generate_canvas(file_name, index):
    file_path = f"{image_url_prefix}/{file_name}"
    canvas_id = f"{image_url_prefix}/canvas{index+1}"
    
    label = file_name.split('_')[1:-1]
    label = "_".join(label).rsplit('.', 1)[0]
    print(label)
    
    return {
        "@id": canvas_id,
        "@type": "sc:Canvas",
        "label": label,
        "height": 3420, 
        "width": 2420,
        "images": [
            {
                "@type": "oa:Annotation",
                "motivation": "sc:painting",
                "resource": {
                    "@id": file_path,
                    "@type": "dctypes:Image",
                    "format": "image/png",
                    "height": 3420,
                    "width": 2420
                },
                "on": canvas_id
            }
        ]
    }

for index, image_file in enumerate(image_files):
    print(f"Processing file: {image_file}")

    manifest['sequences'][0]['canvases'].append(generate_canvas(image_file, index))

output_file = os.path.join(image_directory, 'manifest.json')
with open(output_file, 'w') as f:
    json.dump(manifest, f, indent=4)

print(f"Manifest file created at: {output_file}")
