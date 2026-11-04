"""P11 / P13 / P14: module constants, classes, class attributes, properties."""

from laila.entry import EntryState

SCOPES = ["a", "b"]
LIMIT = 4


class Counter:
    start = 0

    def __init__(self, step=1):
        self.step = step
        self.value = Counter.start

    @property
    def doubled(self):
        return self.value * 2

    def bump(self, times=1):
        for _ in range(times):
            self.value += self.step
        return self

    @staticmethod
    def ready():
        return EntryState.READY
