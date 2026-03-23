"""Decorators for lightweight runtime type coercion."""
from functools import wraps
from inspect import signature


def ensure_list(arg_name: str):
    """Decorator that wraps a single value in a list if it is not already one.

    Parameters
    ----------
    arg_name : str
        Name of the parameter to coerce.

    Returns
    -------
    callable
        Decorator that wraps the target function.
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