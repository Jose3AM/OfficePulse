from pathlib import Path

import boto3


BUCKET_NAME = "officepulse-data-jm-2026"
PREFIX = "raw/office-node-01/"

RAW_DIR = Path("data/raw")


def download_raw_data() -> None:
    """Download OfficePulse raw JSON objects from Amazon S3."""

    RAW_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    s3 = boto3.client(
        "s3",
        region_name="us-east-1",
    )

    paginator = s3.get_paginator(
        "list_objects_v2"
    )

    downloaded = 0
    skipped = 0

    print(
        f"Leyendo s3://{BUCKET_NAME}/{PREFIX}"
    )

    for page in paginator.paginate(
        Bucket=BUCKET_NAME,
        Prefix=PREFIX,
    ):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if not key.endswith(".json"):
                continue

            filename = Path(key).name
            destination = RAW_DIR / filename

            # Evita descargar otra vez archivos
            # que ya existen localmente.
            if destination.exists():
                skipped += 1
                continue

            s3.download_file(
                BUCKET_NAME,
                key,
                str(destination),
            )

            downloaded += 1

    total_local = len(
        list(RAW_DIR.glob("*.json"))
    )

    print("\n=== RAW INGESTION REPORT ===")
    print(f"Nuevos descargados : {downloaded}")
    print(f"Ya existentes      : {skipped}")
    print(f"Total local        : {total_local}")
    print(f"Destino            : {RAW_DIR.resolve()}")


if __name__ == "__main__":
    download_raw_data()