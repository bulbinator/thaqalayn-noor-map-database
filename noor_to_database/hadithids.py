import os
import re
from xml.etree import ElementTree as ET

def get_hadith_ids():
    sitemap_files = "sitemap_files"

    # Namespace used in sitemap XML
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    hadith_ids = set()

    for filename in os.listdir(sitemap_files):
        if filename.endswith(".xml"):
            file_path = os.path.join(sitemap_files, filename)

            tree = ET.parse(file_path)
            root = tree.getroot()

            locs = root.findall("sm:url/sm:loc", ns)

            for loc in locs:
                url = loc.text
                match = re.search(r"/hadith/(\d+)/", url)
                if match:
                    hadith_ids.add(match.group(1))
    
    return sorted(hadith_ids)