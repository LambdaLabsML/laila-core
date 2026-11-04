"""P3 / P4 / P9: first binding is `let`, rebinding is `let mut`; return types; keyword clashes."""


def count(items, type="x"):
    n = 0
    for item in items:
        n += 1
    match = [i for i in items if i == type]
    return n, match


def labels():
    out = []
    out.append("a")
    out = out + ["b"]
    return out
