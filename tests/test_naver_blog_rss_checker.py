from datetime import UTC, datetime, timedelta
import unittest

from dags.naver_blog_blog_rss_checker import (
    INITIAL_BACKFILL_WINDOW,
    RECOVERY_OVERLAP_WINDOW,
    plan_recovery_window,
    should_skip_entry_for_window,
)


class RecoveryWindowPlanningTest(unittest.TestCase):
    def test_new_blog_uses_initial_backfill_window(self):
        now = datetime(2026, 3, 6, 12, 0, tzinfo=UTC)
        mode, cutoff = plan_recovery_window(
            initial_backfill_completed_at=None,
            last_successful_rss_checked_at=None,
            now=now,
        )

        self.assertEqual(mode, "initial_backfill")
        self.assertEqual(cutoff, now - INITIAL_BACKFILL_WINDOW)

    def test_existing_blog_without_last_success_uses_bridge_recovery(self):
        now = datetime(2026, 3, 6, 12, 0, tzinfo=UTC)
        mode, cutoff = plan_recovery_window(
            initial_backfill_completed_at=datetime(2026, 3, 1, 12, 0, tzinfo=UTC),
            last_successful_rss_checked_at=None,
            now=now,
        )

        self.assertEqual(mode, "bridge_recovery")
        self.assertEqual(cutoff, now - INITIAL_BACKFILL_WINDOW)

    def test_existing_blog_uses_last_success_minus_overlap(self):
        now = datetime(2026, 3, 6, 12, 0, tzinfo=UTC)
        last_success = datetime(2026, 3, 6, 10, 30, tzinfo=UTC)
        mode, cutoff = plan_recovery_window(
            initial_backfill_completed_at=datetime(2026, 3, 1, 12, 0, tzinfo=UTC),
            last_successful_rss_checked_at=last_success,
            now=now,
        )

        self.assertEqual(mode, "incremental")
        self.assertEqual(cutoff, last_success - RECOVERY_OVERLAP_WINDOW)


class RecoveryWindowFilteringTest(unittest.TestCase):
    def test_entry_older_than_cutoff_is_skipped(self):
        cutoff = datetime(2026, 3, 6, 9, 0, tzinfo=UTC)
        published_at = cutoff - timedelta(minutes=1)

        self.assertTrue(should_skip_entry_for_window(published_at, cutoff))

    def test_entry_without_published_at_is_never_skipped_by_cutoff(self):
        cutoff = datetime(2026, 3, 6, 9, 0, tzinfo=UTC)

        self.assertFalse(should_skip_entry_for_window(None, cutoff))

    def test_entry_newer_than_cutoff_is_kept(self):
        cutoff = datetime(2026, 3, 6, 9, 0, tzinfo=UTC)
        published_at = cutoff + timedelta(minutes=1)

        self.assertFalse(should_skip_entry_for_window(published_at, cutoff))


if __name__ == "__main__":
    unittest.main()
