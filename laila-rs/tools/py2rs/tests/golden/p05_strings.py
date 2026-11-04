"""P5 / P7 / P10: string literals, f-strings with specs, comments."""


def banner(name, n, ratio):
    # a leading comment
    single = 'single-quoted'
    raw = r"\d+"
    multi = """two
lines"""
    return f"{name:>8}|{n:03d}|{ratio:.2f}|{name!r}|{{lit}}"  # trailing comment
