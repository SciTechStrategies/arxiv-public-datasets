import click
from arxiv_public_data import s3_bulk_download


@click.command()
@click.option(
    '--manifest-filename',
    type=click.Path(file_okay=True, dir_okay=False, writable=True),
)
@click.option(
    '--redownload-manifest',
    is_flag=True,
)
@click.option(
    '--year',
    type=int,
)
@click.option(
    '--month',
    type=int,
)
def cli(
    manifest_filename,
    redownload_manifest,
    year,
    month,
):
    """Download arXiv pdfs."""
    manifest = s3_bulk_download.get_manifest(
        manifest_filename, redownload_manifest
    )
    if year:
        manifest = [
            item for item in manifest if item['timestamp'].startswith(
                str(year) + '-'
            )
        ]
        if month:
            mm = "{:02d}".format(month)
            manifest = [
                item for item in manifest
                if item['timestamp'].split('-')[1] == mm
            ]
    s3_bulk_download.download_check_tarfiles(manifest)


if __name__ == '__main__':
    cli()
