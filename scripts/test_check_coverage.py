import contextlib
import io
import unittest

from check_coverage import check, coverage


class CoverageTests(unittest.TestCase):
    def test_each_package_is_gated_even_when_aggregate_passes(self):
        profile = "mode: atomic\nmodule/root.go:1.1,2.1 99 1\nmodule/internal/a.go:1.1,2.1 1 0\n"
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertFalse(check(profile, 85))
        self.assertEqual(coverage(profile)["TOTAL"], (99, 100))

    def test_threshold_uses_unrounded_statement_counts(self):
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertTrue(check("mode: set\na.go:1.1,2.1 85 1\na.go:3.1,4.1 15 0\n", 85))
            self.assertFalse(check("mode: set\na.go:1.1,2.1 8499 1\na.go:3.1,4.1 1501 0\n", 85))

    def test_duplicate_blocks_merge_without_inflating_statement_count(self):
        profile = "mode: count\na.go:1.1,2.1 10 0\na.go:1.1,2.1 10 3\n"
        self.assertEqual(coverage(profile)["TOTAL"], (10, 10))

    def test_invalid_profiles_fail_closed(self):
        for profile in ("", "mode: atomic\n", "mode: invalid\n", "mode: set\nmalformed\n",
                        "mode: set\na.go:1.1,2.1 -1 0\n", "mode: set\na.go:1.1,2.1 1 -1\n",
                        "mode: set\na.go:1.1,2.1 1 0\na.go:1.1,2.1 2 1\n"):
            with self.subTest(profile=profile), self.assertRaises(ValueError):
                coverage(profile)


if __name__ == "__main__":
    unittest.main()
