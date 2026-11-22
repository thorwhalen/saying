# saying.base

Base functionality for saying package.

### Functions

| `identity`(x)                                                                   |                                                                                                                                              |
|---------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| [`pipe`](#saying.base.pipe)(\*funcs, \*\*named_funcs) | Like `dol.Pipe(...)` but if first "func" is not a callable, will replace it with an argument-less function that will returning this argument |

### saying.base.pipe(\*funcs, \*\*named_funcs)

Like `dol.Pipe(...)` but if first “func” is not a callable,
will replace it with an argument-less function that will returning this argument
