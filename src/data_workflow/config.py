from pathlib import Path

def paths(root):
    data1folder = root / "data"

    return {
        "raw": data1folder / "raw",
        "processed": data1folder / "processed",
        "external":data1folder /"external",
        "cache":data1folder/"cache"

    }