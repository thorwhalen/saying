"""Offline tests for saying.util and saying.wikimedia: pure helpers, bz2 round trip, fake downloads, xml parsing."""

import bz2
import os

import pytest

from saying import util, wikimedia


def test_text_normalisation():
    assert util.lower_alphanumeric("Hello, World!!  Again") == "hello world again"
    assert util.hash_text("A b") == util.hash_text("a, b")
    assert util.remove_surrounding_single_quotes("'x'") == "x"
    assert util.remove_surrounding_single_quotes("x'") == "x'"
    assert util.clean_string("'quoted'") == "quoted"
    assert util.clean_string("  plain  ") == "plain"
    assert util.clean_string("text\\r") == "text"


def test_dict_helpers():
    rows = list(util.dict_of_rows_and_columns([(1, 2), (3, 4)], ["a", "b"]))
    assert rows == [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    assert util.subdict(["a"], {"a": 1, "b": 2}) == {"a": 1}
    assert util.subdict(["a"])({"a": 1, "b": 2}) == {"a": 1}
    assert util.map_fields({"a": "x"}, {"a": 1, "b": 2}) == {"x": 1}
    assert util.map_fields({"a": "x"}, {"a": 1, "b": 2}, strict=False) == {"x": 1, "b": 2}
    assert util.map_fields({"a": "x"})({"a": 1}) == {"x": 1}


def test_remove_duplicates():
    rows = [{"a": 1, "b": 1}, {"a": 1, "b": 2}, {"a": 1, "b": 1}]
    assert len(util.remove_duplicates(rows)) == 2
    assert util.remove_duplicates(rows, keys=["a"]) == [{"a": 1, "b": 1}]


def test_extract_sql_data_accepts_bytes_and_decoder():
    text = "INSERT INTO `q` (a,b) VALUES (1,'x'),(2,'y');"
    assert util.extract_sql_data(text) == [["1", "'x'"], ["2", "'y'"]]
    assert util.extract_sql_data(text.encode()) == [["1", "'x'"], ["2", "'y'"]]
    assert util.extract_sql_data(text.encode(), bytes_decoder=util.lenient_bytes_decoder)


def test_lenient_decoder_and_clog(capsys):
    assert util.lenient_bytes_decoder(b"a\xffb") == "a�b"
    assert util.lenient_bytes_decoder("already") == "already"
    util.clog(True, "shown")
    logger = util.clog(True)
    logger("via partial")
    assert capsys.readouterr().out == "shown\nvia partial\n"


def test_register_graze_url():
    util.register_graze_url("tmp_test_name", "http://example.test/x")
    assert util.graze_urls["tmp_test_name"] == "http://example.test/x"
    del util.graze_urls["tmp_test_name"]


def test_extensions():
    assert util.extension_of("a/b/c.tar.bz2") == ".bz2"
    assert util.without_extension("a/b/c.xml") == "a/b/c"


def test_extract_bz2_roundtrip(tmp_path):
    src = tmp_path / "data.txt.bz2"
    src.write_bytes(bz2.compress(b"hello\nworld\n"))
    out = util.extract_bz2(str(src))
    assert out == str(tmp_path / "data.txt")
    assert open(out, "rb").read() == b"hello\nworld\n"
    bad = tmp_path / "bad.bz2"
    bad.write_bytes(b"not bz2 at all")
    with pytest.raises(OSError, match="Try downloading again"):
        util.extract_bz2(str(bad), str(tmp_path / "bad.out"))


def test_download_bz2_skips_existing_and_downloads(tmp_path, monkeypatch, capsys):
    calls = []

    class Resp:
        content = b"payload"

    import requests

    monkeypatch.setattr(requests, "get", lambda url, verify=True: calls.append(url) or Resp())
    target = str(tmp_path / "f.bz2")
    util.download_bz2("http://x/f.bz2", target)
    assert open(target, "rb").read() == b"payload" and len(calls) == 1
    util.download_bz2("http://x/f.bz2", target)  # exists: no second call
    assert len(calls) == 1 and "already exists" in capsys.readouterr().out


def test_download_and_extract_existing_target_is_left_alone(tmp_path, capsys):
    target = tmp_path / "out.txt"
    target.write_text("x")
    util.download_and_extract("http://x/out.txt.bz2", str(target))
    assert "already exists" in capsys.readouterr().out


XML = """<mediawiki xmlns="http://www.mediawiki.org/xml/export-0.10/">
<page><title>Ada</title><revision><text>* ''First quote''
* ''Second quote''</text></revision></page>
<page><title>Empty</title><revision><text></text></revision></page>
<page><title>NoRevision</title></page>
</mediawiki>"""


def test_parse_wikimedia_xml(tmp_path):
    f = tmp_path / "pages.xml"
    f.write_text(XML)
    assert wikimedia.parse_wikimedia_xml(str(f)) == [
        ("Ada", "First quote"),
        ("Ada", "Second quote"),
    ]
