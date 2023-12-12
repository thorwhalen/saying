"""Get quotes data etc."""

import os
from typing import Iterable, Optional
from config2py import get_app_data_folder
from saying.util import (
    clog,
    package_name,
    download_and_extract,
    DLFT_DATA_DIR,
)

DLFT_WIKI_QUOTES_DATA_DIR = get_app_data_folder(
    os.path.join(DLFT_DATA_DIR, 'wikimedia'), ensure_exists=True
)


def download_and_process_wiki_data(
    languages: str | Iterable[str] = "en",
    data_dir: str = DLFT_WIKI_QUOTES_DATA_DIR,
    *,
    date: str = "20231201",  # TODO: Get most recent automatically from https://dumps.wikimedia.org/{language}wikiquote/
    datedbpedia2: str = "2022.12.01",  # TODO: Get most recent automatically from https://downloads.dbpedia.org/repo/dbpedia/wikidata/sameas-all-wikis/
    verbose=True,
):
    """
    Downloads and processes quotation data from wikimedia and dbpedia.

    If you have trouble with it, try running again, and if problems persist, download
    the needed resources manually and place them in the data_dir.

    You'll find them here: https://dumps.wikimedia.org/enwikiquote/

    """

    import os

    if isinstance(languages, str):
        # extract words (\w+) from string
        import re

        languages = re.findall(r'\w+', languages)

    clog(verbose, "DATE:", date)

    os.chdir(data_dir)

    # Download and extract ids.ttl.bz2
    download_and_extract(
        f"https://downloads.dbpedia.org/repo/dbpedia/wikidata/sameas-all-wikis/{datedbpedia2}/sameas-all-wikis.ttl.bz2",
        "ids.ttl",
        verbose=verbose,
    )

    # Process each language
    for language in languages:
        if verbose:
            print(f"\n{language=}")

        url_base = f"https://dumps.wikimedia.org/{language}wikiquote/{date}/{language}wikiquote-{date}-"
        language_dir = os.path.join(data_dir, language)
        os.makedirs(language_dir, exist_ok=True)

        # Download and extract wikidata.sql.gz and pages.xml.bz2
        download_and_extract(
            url_base + "wbc_entity_usage.sql.gz",
            os.path.join(language_dir, "wikidata.sql"),
            verbose=verbose,
        )
        download_and_extract(
            url_base + "pages-meta-current.xml.bz2",
            os.path.join(language_dir, "pages.xml"),
            verbose=verbose,
        )

        # Additional processing steps can be added here as per the original script

    if verbose:
        print("Done.")


import xml.etree.ElementTree as ET
import re


def parse_wikimedia_xml(file_path):
    """Parse (author, quote_text) pairs from xml file.
     The xml file is assumped to be one that has been downloaded from wikimedia
     (https://dumps.wikimedia.org/enwikiquote/).
     For example, using the `download_and_process_wiki_data` function.

    Note the relative filepath has, at the time of writing this, the format
    `"{language}/pages.xml"` (e.g. `"en/pages.xml"`)

    """
    tree = ET.parse(file_path)
    root = tree.getroot()

    quotes_data = []

    for page in root.findall('{http://www.mediawiki.org/xml/export-0.10/}page'):
        title_elem = page.find('{http://www.mediawiki.org/xml/export-0.10/}title')
        text_elem = page.find(
            '{http://www.mediawiki.org/xml/export-0.10/}revision/{http://www.mediawiki.org/xml/export-0.10/}text'
        )

        # if title_elem is not None and text_elem is not None:
        #     title = title_elem.text
        #     text = text_elem.text

        #     # Basic parsing for quotes (may need adjustment based on the actual format)
        #     quotes = re.findall(r'\*\s*\'\'(.+?)\'\'', text)
        #     for quote in quotes:
        #         quotes_data.append((title, quote))

        if (
            title_elem is not None
            and text_elem is not None
            and text_elem.text is not None
        ):
            title = title_elem.text
            text = text_elem.text

            # Basic parsing for quotes (may need adjustment based on the actual format)
            quotes = re.findall(r'\*\s*\'\'(.+?)\'\'', text)
            for quote in quotes:
                quotes_data.append((title, quote))

    return quotes_data
