import csv
import os
import requests
import tarfile

from config import BLOB_URL


def fixed_width_to_csv(content: str, out_path: str):
    """
    Parse a KNMI fixed-width file and write it as CSV.
    Column positions are detected dynamically from the '# DTG' header line
    so format changes across years are handled automatically.
    """
    col_names = col_starts = None

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = None
        for line in content.splitlines():
            if line.startswith("# DTG"):
                stripped = line[2:]
                col_names, col_starts = [], []
                i = 0
                while i < len(stripped):
                    if stripped[i] != " ":
                        j = i
                        while j < len(stripped) and stripped[j] != " ":
                            j += 1
                        col_names.append(stripped[i:j])
                        col_starts.append(i + 2)
                        i = j
                    else:
                        i += 1
                writer = csv.writer(f)
                writer.writerow(col_names)
            elif line.startswith("#"):
                continue
            elif col_starts and line.strip():
                row = []
                for k, start in enumerate(col_starts):
                    end = col_starts[k + 1] if k + 1 < len(col_starts) else len(line)
                    row.append(line[start:end].strip() if start < len(line) else "")
                writer.writerow(row)


def extract(extract_dir: str, target_months: set):
    """Stream the blob and extract target monthly files as CSVs to extract_dir."""
    print(f"Extracting to: {extract_dir}")
    with requests.get(BLOB_URL, stream=True) as r:
        r.raise_for_status()
        with tarfile.open(fileobj=r.raw, mode="r|gz") as tar:
            for member in tar:
                if "kis_tot_" not in member.name:
                    continue
                month = member.name.split("kis_tot_")[-1].strip()
                if month not in target_months:
                    continue
                content = tar.extractfile(member).read().decode("utf-8", errors="replace")
                fixed_width_to_csv(content, os.path.join(extract_dir, f"kis_tot_{month}.csv"))
                print(f"  ✓ {month}")
    print(f"Done. {len(os.listdir(extract_dir))} files extracted.")
