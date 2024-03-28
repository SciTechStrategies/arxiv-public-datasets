import glob
import gzip
import json
import logging
import os
import subprocess
import tempfile
from concurrent.futures import (
    ThreadPoolExecutor,
    TimeoutError,
)
from typing import Dict, List, Optional, Set, TextIO

import click
import pebble
import refextract

from arxiv_public_data import s3_bulk_download

EXTRACT_REFS_TIMEOUT_SECONDS = 5 * 60  # How long to wait for ref extraction
NUM_DOWNLOAD_THREADS = 4  # Number of tars to download in parallel

logger = logging.getLogger(__name__)


@click.command()
@click.option('--from-year', type=int, required=True)
@click.option('--from-month', type=int, required=True)
@click.option('--until-year', type=int, required=True)
@click.option('--until-month', type=int, required=True)
@click.option('--output-base-dir', default='data')
@click.option('--redownload-manifest', is_flag=True)
@click.option('--already-downloaded-tars', type=click.File('rt'))
def cli(
    from_year: int,
    from_month: int,
    until_year: int,
    until_month: int,
    output_base_dir: str,
    redownload_manifest: bool = False,
    already_downloaded_tars: Optional[TextIO] = None,
):
    already_downloaded = set(
        line.strip()
        for line in already_downloaded_tars
    ) if already_downloaded_tars else set()

    os.makedirs(output_base_dir, exist_ok=True)
    # Get manifest
    manifest_filename = os.path.join(output_base_dir, "manifest.xml")
    manifest = s3_bulk_download.get_manifest(
        manifest_filename, redownload_manifest
    )
    for year, month in year_month_iter(
        from_year=from_year,
        from_month=from_month,
        until_year=until_year,
        until_month=until_month,
    ):
        # Iterate over months
        extract_refs_for_month(
            year=year,
            month=month,
            output_base_dir=output_base_dir,
            manifest=manifest,
            already_downloaded=already_downloaded,
        )


def extract_refs_for_month(
    *,
    year: int,
    month: int,
    output_base_dir: str,
    manifest: List[Dict],
    already_downloaded: Optional[Set[str]] = None,
):
    logger.info("Extracting refs for year/month: %d/%d", year, month)
    ym_prefix = "{}-{:02d}".format(year, month)
    refs_output_dir = os.path.join(
        output_base_dir,
        ym_prefix,
        "refs",
    )
    os.makedirs(refs_output_dir, exist_ok=True)
    tar_files_to_download = [
        item['filename'] for item in manifest
        if (
            item['timestamp'].startswith(ym_prefix)
            and item['filename'] not in already_downloaded
        )
    ]
    with ThreadPoolExecutor(max_workers=NUM_DOWNLOAD_THREADS) as ex:
        for tar_filename in tar_files_to_download:
            ex.submit(
                extract_refs_for_tarfile,
                tar_filename,
                refs_output_dir,
            )


def extract_refs_for_tarfile(
    s3_url: str,
    output_dir: str,
):
    logger.info("Extracting refs for tarfile %s to %s", s3_url, output_dir)
    with tempfile.TemporaryDirectory() as td:
        tar_filename = os.path.join(td, "pdfs.tgz")
        pdfs_dir = os.path.join(td, "pdfs")
        os.makedirs(pdfs_dir, exist_ok=True)
        s3_bulk_download.download_file(s3_url, tar_filename)
        subprocess.run(["tar", "-xvzf", tar_filename, "-C", pdfs_dir])

        pool = pebble.ProcessPool()
        pdf_filenames = glob.glob("{}/*/*.pdf".format(pdfs_dir))
        futures = {}
        for pdf_filename in pdf_filenames:
            future = pool.schedule(
                extract_refs_for_pdf,
                (pdf_filename, output_dir),
                timeout=EXTRACT_REFS_TIMEOUT_SECONDS,
            )
            futures[pdf_filename] = future
        pool.close()
        pool.join()
        for pdf_filename, future in futures.items():
            try:
                future.result()
            except TimeoutError:
                logger.warning(
                    "Timeout for ref extraction %s", pdf_filename
                )
    logger.info("Tarfile extraction complete: %s", s3_url)


def extract_refs_for_pdf(pdf_filename: str, output_dir: str):
    logger.info("Extracting refs for pdf %s to %s", pdf_filename, output_dir)
    output_filename = os.path.join(
        output_dir,
        os.path.basename(pdf_filename).replace('.pdf', '-refs.json.gz')
    )
    references = refextract.extract_references_from_file(pdf_filename)
    with gzip.open(output_filename, 'wt') as ofh:
        json.dump(references, ofh)


def year_month_iter(*, from_month, from_year, until_month, until_year):
    ym_start = 12 * from_year + from_month - 1
    ym_end = 12 * until_year + until_month - 1
    for ym in range(ym_start, ym_end):
        y, m = divmod(ym, 12)
        yield y, m + 1


if __name__ == "__main__":
    cli()
