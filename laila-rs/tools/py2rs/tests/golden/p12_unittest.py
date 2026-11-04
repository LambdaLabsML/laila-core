"""P12: unittest.TestCase subclasses, setUp / tearDown, assertRaises, subTest."""

import unittest

import laila


class TestEntryBasics(unittest.TestCase):
    def setUp(self):
        self.e = laila.constant(data=[1, 2])

    def tearDown(self):
        laila.forget(self.e)

    def test_data(self):
        self.assertEqual(self.e.data, [1, 2])
        self.assertIsNone(self.e.evolution)

    def test_raises(self):
        with self.assertRaises(KeyError) as cm:
            _ = {}["k"]
        self.assertEqual(str(cm.exception), "'k'")

    def test_subtests(self):
        for i in range(3):
            with self.subTest(i=i):
                self.assertLess(i, 3)


if __name__ == "__main__":
    unittest.main()
