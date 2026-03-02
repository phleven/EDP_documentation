from pathlib import Path
import zipfile
import os

def unzip_file(zip_path: str, dest_dir: str) -> str:
    """
    Unzip a .zip from a local path or Unity Catalog Volume path (/Volumes/...).
    Example paths:
      zip_path:  /Volumes/<catalog>/<schema>/<volume>/in/file.zip
      dest_dir:  /Volumes/<catalog>/<schema>/<volume>/out/file/
    """
    zip_p = Path(zip_path)
    dest_p = Path(dest_dir)
    dest_p.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_p, "r") as zf:
        # Prevent path traversal ("zip slip")
        for member in zf.infolist():
            target = (dest_p / member.filename).resolve()
            if not str(target).startswith(str(dest_p.resolve()) + os.sep):
                raise ValueError(f"Unsafe path in zip: {member.filename}")
        zf.extractall(dest_p)

    return str(dest_p)

# Example (Unity Catalog Volume)
zip_path = "downloads/2025_TEOS_XML_01A.zip"
dest_dir = "/Volumes/irs_group_catalog/lakebase/files/downloads/2025_TEOS_XML_01A"
print(unzip_file(zip_path, dest_dir))
