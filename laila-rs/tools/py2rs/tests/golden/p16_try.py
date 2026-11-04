"""P16 / P16a: try / except / finally; raise; nested def."""


def safe_div(a, b):
    try:
        return a / b
    except ZeroDivisionError as e:
        return f"error: {e}"
    finally:
        print("done")


def outer(xs):
    def inner(x):
        if x < 0:
            raise ValueError("negative")
        return x * 2

    return [inner(x) for x in xs]
