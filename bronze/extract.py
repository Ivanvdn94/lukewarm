"""
Bronze layer — raw data ingestion.

Streams the KNMI hourly observation archive (.tgz) from Azure Blob Storage and
extracts only the requested monthly files, converting each from the KNMI
fixed-width format to CSV.

Nothing in this module alters, filters, or interprets the data values — that is
the responsibility of the Silver layer. The only transformation here is layout:
fixed-width columns become comma-separated columns.
"""

import csv
import os
import requests
import tarfile

from config import BLOB_URL


def fixed_width_to_csv(content: str, out_path: str):
    """
    Parse a single KNMI fixed-width observation file and write it as a CSV.

    KNMI files use a fixed-width layout where every column starts at a fixed
    character position defined by the header line (prefixed with '# DTG').
    Column positions are detected dynamically from that header so the function
    handles format changes across archive years without hardcoding offsets.

    Comment lines (starting with '#') are skipped. All other non-empty lines
    are sliced at the detected offsets and written as CSV rows.

    Args:
        content:  Full text content of one KNMI fixed-width file.
        out_path: Destination file path for the output CSV.
    """
    col_names = col_starts = None

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = None
        for line in content.splitlines():
            if line.startswith("# DTG"):
                # Strip the leading '# ' and parse column names and their
                # start positions from the header line.
                stripped = line[2:]
                col_names, col_starts = [], []
                i = 0
                while i < len(stripped):
                    if stripped[i] != " ":
                        j = i
                        while j < len(stripped) and stripped[j] != " ":
                            j += 1
                        col_names.append(stripped[i:j])
                        col_starts.append(i + 2)  # +2 accounts for stripped '# '
                        i = j
                    else:
                        i += 1
                writer = csv.writer(f)
                writer.writerow(col_names)
            elif line.startswith("#"):
                # Skip other comment lines (source info, units, etc.)
                continue
            elif col_starts and line.strip():
                # Slice each field at its detected start position.
                # The last column runs to end-of-line.
                row = []
                for k, start in enumerate(col_starts):
                    end = col_starts[k + 1] if k + 1 < len(col_starts) else len(line)
                    row.append(line[start:end].strip() if start < len(line) else "")
                writer.writerow(row)


def extract(extract_dir: str, target_months: set):
    """
    Stream the KNMI archive from Azure Blob and write target months as CSVs.

    The archive is a single .tgz containing one file per month going back to
    the 1950s. It is streamed rather than downloaded in full because the
    complete archive is large and only a small subset of months is typically
    needed. tarfile's streaming mode ('r|gz') reads sequentially, so members
    are processed as they arrive without buffering the entire archive in memory.

    Only files whose month code (e.g. '200307') appears in target_months are
    extracted. All others are skipped without reading their content.

    Args:
        extract_dir:    Local directory where extracted CSV files are written.
        target_months:  Set of month strings in 'YYYYMM' format to extract
                        (e.g. {'200307', '200308'}).
    """
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
