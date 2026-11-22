# saying.util

Utils for saying package.

### Functions

| `clean_string`(s)                                                                            |                                                                                                                               |
|----------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| [`clog`](#saying.util.clog)(condition, \*args[, log_func])         | Conditional log                                                                                                               |
| `dict_of_rows_and_columns`(rows, columns)                                                    |                                                                                                                               |
| `download_and_extract`(url, filename, \*[, verbose])                                         |                                                                                                                               |
| `download_bz2`(url, filename, \*[, verbose])                                                 |                                                                                                                               |
| `extension_of`(filename)                                                                     |                                                                                                                               |
| `extract_bz2`(bz_file[, extract_to_file, ...])                                               |                                                                                                                               |
| [`extract_sql_data`](#saying.util.extract_sql_data)(text, \*[, bytes_decoder]) | Extracts data from sql dump text: one `[field, ...]` list per row of each <br/><br/>```<br/>``<br/>```<br/><br/>INSERT INTO . |
| [`hash_text`](#saying.util.hash_text)(text)                             | Return a hash of the text, ignoring punctuation and capitalization.                                                           |
| `lenient_bytes_decoder`(bytes_)                                                              |                                                                                                                               |
| `lower_alphanumeric`(text)                                                                   |                                                                                                                               |
| `map_fields`(field_map[, d, strict])                                                         |                                                                                                                               |
| `register_graze_url`(name, url)                                                              |                                                                                                                               |
| `remove_duplicates`(list_of_dicts[, keys])                                                   |                                                                                                                               |
| `remove_surrounding_single_quotes`(s)                                                        |                                                                                                                               |
| `subdict`(keys[, d])                                                                         |                                                                                                                               |
| `without_extension`(filename)                                                                |                                                                                                                               |

### saying.util.clog(condition, \*args, log_func=<built-in function print>, \*\*kwargs)

Conditional log

```pycon
>>> clog(False, "logging this")
>>> clog(True, "logging this")
logging this
```

### saying.util.extract_sql_data(text, \*, bytes_decoder=<method 'decode' of 'bytes' objects>)

Extracts data from sql dump text: one `[field, ...]` list per row of each
`INSERT INTO ... VALUES (...),(...);` statement.

Rows are recognised even when the `),(` separators are pretty-printed
across lines (as MySQL dumps often are):

```pycon
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
```

### saying.util.hash_text(text)

Return a hash of the text, ignoring punctuation and capitalization.

```pycon
>>> hash_text('Hello, world!') == hash_text('hello world')
True
>>> hash_text('Hello, world!')
'5eb63bbbe01eeed093cb22bb8f5acdc3'
```
