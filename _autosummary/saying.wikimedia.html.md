# saying.wikimedia

Get quotes data etc.

### Functions

| [`download_and_process_wiki_data`](#saying.wikimedia.download_and_process_wiki_data)([languages, ...])   | Downloads and processes quotation data from wikimedia and dbpedia.   |
|-----------------------------------------------------------------------------------------------------|----------------------------------------------------------------------|
| [`parse_wikimedia_xml`](#saying.wikimedia.parse_wikimedia_xml)(file_path)                     | Parse (author, quote_text) pairs from xml file.                      |

### saying.wikimedia.download_and_process_wiki_data(languages='en', data_dir='/home/runner/.config/saying/wikimedia', , date='20231201', datedbpedia2='2022.12.01', verbose=True)

Downloads and processes quotation data from wikimedia and dbpedia.

If you have trouble with it, try running again, and if problems persist, download
the needed resources manually and place them in the data_dir.

You’ll find them here: [https://dumps.wikimedia.org/enwikiquote/](https://dumps.wikimedia.org/enwikiquote/)

### saying.wikimedia.parse_wikimedia_xml(file_path)

Parse (author, quote_text) pairs from xml file.
: The xml file is assumped to be one that has been downloaded from wikimedia
  ([https://dumps.wikimedia.org/enwikiquote/](https://dumps.wikimedia.org/enwikiquote/)).
  For example, using the `download_and_process_wiki_data` function.

Note the relative filepath has, at the time of writing this, the format
`"{language}/pages.xml"` (e.g. `"en/pages.xml"`)
