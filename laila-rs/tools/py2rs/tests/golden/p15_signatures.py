"""P15 / P20: defaults, *args, **kwargs, keyword-only parameters."""


def memo(*args, persist=True, **kwargs):
    if not args and "entries" in kwargs:
        args = (kwargs.pop("entries"),)
    return [a for a in args], persist, kwargs


def kw_only(a, *, b=2, c=None):
    return a, b, c


def forward(*args, **kwargs):
    return memo(*args, persist=False, **kwargs)
