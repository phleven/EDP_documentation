import requests
import re
import os
from typing import Iterable, List


def download_file(url: str, save_path: str):
    """
    Downloads a file from the given URL and saves it to the specified path.
    Includes error handling and streaming for large files.
    """
    try:
        # Send GET request with streaming enabled
        with requests.get(url, stream=True, timeout=15) as response:
            response.raise_for_status()  # Raise error for bad status codes
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            
            # Write file in chunks to avoid memory issues
            with open(save_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:  # Filter out keep-alive chunks
                        file.write(chunk)
        
        print(f"✅ File downloaded successfully: {save_path}")
    
    except requests.exceptions.RequestException as e:
        print(f"❌ Download failed: {e}")

def substitute_url_suffixes(original_url: str, new_suffixes: Iterable[str]) -> List[str]:
    """
    Given a URL ending with _<suffix>.zip (e.g., ..._01A.zip),
    replace that <suffix> with each value in new_suffixes and return the new URLs.

    Example:
      original_url = "..._01A.zip"
      new_suffixes = ["01A", "02A", "12A"]
      -> ["..._01A.zip", "..._02A.zip", "..._12A.zip"]
    """
    m = re.search(r"_(?P<suffix>[^/_]+)\.zip$", original_url)
    if not m:
        raise ValueError("original_url must end with _<suffix>.zip")

    prefix = original_url[: m.start()]  # everything before the final "_<suffix>.zip"
    return [f"{prefix}_{s}A.zip" for s in new_suffixes]

def url_to_download_path(url: str, downloads_dir: str = "downloads") -> str:
    # Remove querystring/fragment if present
    url = url.split("?", 1)[0].split("#", 1)[0]

    filename = url.rsplit("/", 1)[-1]
    if not filename:
        raise ValueError("URL does not contain a filename")

    return f"{downloads_dir.rstrip('/')}/{filename}"


suffixes = ["01","02","03","04","05","06","07","08","09","10","11","12"]
url = "https://apps.irs.gov/pub/epostcard/990/xml/2025/2025_TEOS_XML_01A.zip" #TODO replace if want to download a different year

urls = substitute_url_suffixes(url, suffixes)
print(urls)
for url in urls:
    # Example usage
    download_file(
        url,
        url_to_download_path(url)
    )
