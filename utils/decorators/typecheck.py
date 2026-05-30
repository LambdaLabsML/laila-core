"""Decorators for lightweight runtime type coercion.

Currently exports only :func:`ensure_list`, which removes the
boilerplate at the top of every "accept either a single item or an
iterable" function. The decorator inspects the wrapped function's
signature, finds the named parameter, and -- if its bound value is
not a :class:`list` / :class:`set` / :class:`frozenset` -- wraps it
in a single-element list before forwarding the call.
"""

from functools import wraps
from inspect import signature


def ensure_list(arg_name: str):
    """Wrap a single value in a list when *arg_name* is not iterable.

    Useful for relaxing function signatures so callers can pass
    either ``f(x)`` or ``f([x, y, z])`` without the function having
    to special-case the scalar form.

    Sets and frozensets are *passed through unchanged* (they are
    already iterable collections); everything else -- including
    plain strings, which are iterable but rarely intended as a
    sequence in this context -- is wrapped in a single-element list.

    Parameters
    ----------
    arg_name : str
        The name of the parameter on the wrapped function whose
        value should be coerced. Must be a real parameter of the
        wrapped function (raises :class:`TypeError` at call time
        if not).

    Returns
    -------
    callable
        A decorator. Apply to the target function.

    Raises
    ------
    TypeError
        At call time, if *arg_name* is not bound by the call (e.g.
        the parameter is missing from the function's signature).
    """

    def decorator(fn):
        sig = signature(fn)

        @wraps(fn)
        def wrapper(*args, **kwargs):
            bound = sig.bind_partial(*args, **kwargs)
            bound.apply_defaults()

            if arg_name not in bound.arguments:
                raise TypeError(f"Argument '{arg_name}' not found in {fn.__name__}")

            value = bound.arguments[arg_name]

            if not isinstance(value, (list, set, frozenset)):
                bound.arguments[arg_name] = [value]

            return fn(*bound.args, **bound.kwargs)

        return wrapper

    return decorator
