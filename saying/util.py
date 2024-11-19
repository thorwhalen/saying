"""Utils for saying package."""

import os
from functools import partial
from typing import Iterable, Dict, KT, VT
from config2py import get_app_data_folder
from graze import graze as _graze
import re

package_name = 'saying'

DFLT_DATA_DIR = get_app_data_folder(package_name, ensure_exists=True)


non_alphanumeric_re = re.compile(r'\W+')


# TODO: Make a non-pandas version to not have to depend on pandas
def remove_duplicates(list_of_dicts, keys=None):
    import pandas as pd

    return pd.DataFrame(list_of_dicts).drop_duplicates(keys).to_dict(orient='records')


def lower_alphanumeric(text):
    return non_alphanumeric_re.sub(' ', text).strip().lower()


def hash_text(text):
    """Return a hash of the text, ignoring punctuation and capitalization.

    >>> (assert hash_text('Hello, world!')
    ...     ==  hash_text('hello world')
    ...     == '5eb63bbbe01eeed093cb22bb8f5acdc3'
    ... )

    """
    from hashlib import md5

    normalized_text = lower_alphanumeric(text)
    return md5(normalized_text.encode()).hexdigest()


def remove_surrounding_single_quotes(s):
    if s.startswith("'") and s.endswith("'"):
        return s[1:-1]
    return s


def clean_string(s):
    s = remove_surrounding_single_quotes(s)
    if s.endswith("\\r"):
        s = s[:-2]
    s = s.strip()
    return s


def dict_of_rows_and_columns(rows, columns):
    for row in rows:
        yield dict(zip(columns, row))


def subdict(keys: Iterable[KT], d: dict = None):
    if d is None:
        return partial(subdict, tuple(keys))
    return {k: d[k] for k in keys}


def map_fields(field_map: Dict[KT, KT], d: Dict[KT, VT] = None, *, strict=True):
    if d is None:
        return partial(map_fields, field_map, strict=strict)
    if strict:
        d = subdict(field_map.keys(), d)
    return {field_map.get(k, k): v for k, v in d.items()}


def extract_sql_data(text, *, bytes_decoder=bytes.decode):
    """
    Extracts data from sql dump text.

    >>> text = '''
    ... -- MySQL dump 10.13  ...
    ... ...
    ... INSERT INTO `quotes` (col1,col2,col3) VALUES
    ... (1,'One', 1.0),
    ... (2,'Two', 2.0),
    ... (3,'Three', 3.0);
    ... '''
    >>> extract_sql_data(text)
    [['1', "'One'", '1.0'], ['2', "'Two'", '2.0'], ['3', "'Three'", '3.0']]
    [['1', "'One'", '1.0)', '(2', "'Two'", '2.0)', '(3', "'Three'", '3.0']]
    """
    import re

    def extract_sql_table_rows(text, *, bytes_decoder=bytes_decoder):
        if isinstance(text, bytes):
            text = bytes_decoder(text)
        # Find all INSERT INTO statements
        insert_statements = re.findall(
            (
                r"INSERT INTO "
                r"`[^`]+"  # table name
                r"`.+?"  # could be nothing, or colmns...
                r"VALUES.+?\((.*?)\);"  # statement (rows)
            ),
            text,
            re.DOTALL,
        )

        for statement in insert_statements:
            # Split the statement into individual records
            records = statement.split('),(')
            for record in records:
                # Clean and split the record into fields
                fields = record.strip().split(',')
                # Clean up each field (remove surrounding quotes and escape sequences)
                cleaned_fields = [
                    re.sub(r"(^' | '$)", '', field).replace("\\'", "'").strip()
                    for field in fields
                ]
                yield cleaned_fields

    return list(extract_sql_table_rows(text, bytes_decoder=bytes_decoder))


graze = partial(_graze, rootdir=DFLT_DATA_DIR)

graze_urls = {
    'micheleriva_5421': 'https://raw.githubusercontent.com/micheleriva/the-quotes-database/master/src/data/quotes.json',
    'x16bkkamz6rkb78rzt7op_75968': 'https://raw.githubusercontent.com/x16bkkamz6rkb78rzt7op/englishquotesdatabase/master/quotesdb.sql',
}


def register_graze_url(name, url):
    graze_urls[name] = url


names = set(graze_urls.keys())


def lenient_bytes_decoder(bytes_: bytes):
    if isinstance(bytes_, bytes):
        return bytes_.decode('utf-8', 'replace')
    return bytes_


def clog(condition, *args, log_func=print, **kwargs):
    """Conditional log

    >>> clog(False, "logging this")
    >>> clog(True, "logging this")
    logging this

    """
    if not args and not kwargs:
        import functools

        return functools.partial(clog, condition, log_func=log_func)
    if condition:
        return log_func(*args, **kwargs)


def download_bz2(url, filename, *, verbose=True):
    import requests

    if not os.path.isfile(filename):
        if verbose:
            print(f"Downloading {url} to {filename}")
        response = requests.get(url, verify=False)
        with open(filename, 'wb') as file:
            file.write(response.content)
    else:
        clog(verbose, f"File {filename} already exists. Skipping download.")


def extension_of(filename):
    _, ext = os.path.splitext(filename)
    return ext


def without_extension(filename):
    filename_, *_ = os.path.splitext(filename)
    return filename_


def extract_bz2(bz_file, extract_to_file=None, *, rm_bz2_after_extraction=True):
    import bz2

    extract_to_file = extract_to_file or without_extension(bz_file)
    try:
        with bz2.BZ2File(bz_file, 'rb') as f_in, open(extract_to_file, 'wb') as f_out:
            f_out.writelines(f_in)
    except OSError as e:
        raise OSError(
            f"Error extracting {bz_file} to {extract_to_file}. Try downloading again."
        ) from e
    return extract_to_file


def download_and_extract(url, filename, *, verbose=True):
    ext = extension_of(url)
    if not os.path.isfile(filename):
        bz_file = filename + ext
        if not os.path.isfile(bz_file):
            download_bz2(url, bz_file)
        else:
            clog(verbose, f"File {bz_file} already exists. Skipping download.")
        filename = extract_bz2(bz_file)
    else:
        clog(verbose, f"File {filename} already exists. Skipping download.")
