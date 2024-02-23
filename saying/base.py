"""Base functionality for saying package."""

from functools import partial
from dol import Pipe, FuncReader, add_ipython_key_completions

from saying.util import lenient_bytes_decoder, extract_sql_data, graze


def identity(x):
    return x


def _if_first_element_not_callable_replace_with_factory(funcs):
    if funcs and not callable(funcs[0]):
        func_input = funcs[0]
        funcs = (partial(identity, func_input),) + funcs[1:]
    return funcs


def pipe(*funcs, **named_funcs):
    """Like `dol.Pipe(...)` but if first "func" is not a callable,
    will replace it with an argument-less function that will returning this argument
    """
    funcs = _if_first_element_not_callable_replace_with_factory(funcs)
    return Pipe(*funcs, **named_funcs)


# Explicit source-to-object definition of the quotes sources
# TODO: Some opportunities for declarative-compression (routing)
sources = {
    'micheleriva_5421': (
        'https://raw.githubusercontent.com/micheleriva/the-quotes-database/master/src/data/quotes.json',
        graze,
        lenient_bytes_decoder,
        __import__('json').loads,
    ),
    'englishquotesdatabase_75968': (
        'https://raw.githubusercontent.com/x16bkkamz6rkb78rzt7op/englishquotesdatabase/master/quotesdb.sql',
        graze,
        lenient_bytes_decoder,
        extract_sql_data,
    ),
}

quotes_src = add_ipython_key_completions(
    FuncReader({k: pipe(*v) for k, v in sources.items()})
)
