import datetime

from cnodc.nodb import NODBQueueItem, QueueStatus
from helpers.base_test_case import BaseTestCase


class TestQueueItem(BaseTestCase):

    def test_mark_complete(self):
        qi = NODBQueueItem(
            queue_uuid='12345',
            priority=1,
            escalation_level=2,
            status=QueueStatus.LOCKED,
            locked_by="me",
            locked_since=datetime.datetime(2026,3,3,5,0,3),
            is_new=True
        )
        self.db.insert_object(qi)
        qi.mark_complete(self.db)
        self.assertIs(qi.status, QueueStatus.COMPLETE)
        self.assertIsNone(qi.locked_by)
        self.assertIsNone(qi.locked_since)
        self.assertEqual(qi.priority, 1)
        self.assertEqual(qi.escalation_level, 2)
        self.assertIsNone(qi.delay_release)
        qi2 = NODBQueueItem.find_by_uuid(self.db, '12345')
        self.assertIs(qi2.status, QueueStatus.COMPLETE)
        self.assertIsNone(qi2.locked_by)
        self.assertIsNone(qi2.locked_since)
        self.assertEqual(qi2.priority, 1)
        self.assertEqual(qi2.escalation_level, 2)
        self.assertIsNone(qi2.delay_release)

    def test_mark_failed(self):
        qi = NODBQueueItem(
            queue_uuid='12345',
            priority=1,
            escalation_level=2,
            status=QueueStatus.LOCKED,
            locked_by="me",
            locked_since=datetime.datetime(2026,3,3,5,0,3),
            is_new=True
        )
        self.db.insert_object(qi)
        qi.mark_failed(self.db)
        self.assertIs(qi.status, QueueStatus.ERROR)
        self.assertIsNone(qi.locked_by)
        self.assertIsNone(qi.locked_since)
        self.assertEqual(qi.priority, 1)
        self.assertEqual(qi.escalation_level, 2)
        self.assertIsNone(qi.delay_release)
        qi2 = NODBQueueItem.find_by_uuid(self.db, '12345')
        self.assertIs(qi2.status, QueueStatus.ERROR)
        self.assertIsNone(qi2.locked_by)
        self.assertIsNone(qi2.locked_since)
        self.assertEqual(qi2.priority, 1)
        self.assertEqual(qi2.escalation_level, 2)
        self.assertIsNone(qi2.delay_release)

    def test_release_no_delay(self):
        qi = NODBQueueItem(
            queue_uuid='12345',
            priority=1,
            escalation_level=2,
            status=QueueStatus.LOCKED,
            locked_by="me",
            locked_since=datetime.datetime(2026,3,3,5,0,3),
            is_new=True
        )
        self.db.insert_object(qi)
        qi.release(self.db)
        self.assertIs(qi.status, QueueStatus.UNLOCKED)
        self.assertIsNone(qi.locked_by)
        self.assertIsNone(qi.locked_since)
        self.assertEqual(qi.priority, 1)
        self.assertEqual(qi.escalation_level, 2)
        self.assertIsNone(qi.delay_release)
        qi2 = NODBQueueItem.find_by_uuid(self.db, '12345')
        self.assertIs(qi2.status, QueueStatus.UNLOCKED)
        self.assertIsNone(qi2.locked_by)
        self.assertIsNone(qi2.locked_since)
        self.assertEqual(qi2.priority, 1)
        self.assertEqual(qi2.escalation_level, 2)
        self.assertIsNone(qi2.delay_release)

    def test_release_delay(self):
        qi = NODBQueueItem(
            queue_uuid='12345',
            priority=1,
            escalation_level=2,
            status=QueueStatus.LOCKED,
            locked_by="me",
            locked_since=datetime.datetime(2026,3,3,5,0,3),
            is_new=True
        )
        self.db.insert_object(qi)
        qi.release(self.db, 60)
        self.assertIs(qi.status, QueueStatus.DELAYED_RELEASE)
        self.assertIsNone(qi.locked_by)
        self.assertIsNone(qi.locked_since)
        self.assertEqual(qi.priority, 1)
        self.assertEqual(qi.escalation_level, 2)
        self.assertIsNotNone(qi.delay_release)
        self.assertGreater(qi.delay_release, datetime.datetime.now(datetime.timezone.utc))
        qi2 = NODBQueueItem.find_by_uuid(self.db, '12345')
        self.assertIs(qi2.status, QueueStatus.DELAYED_RELEASE)
        self.assertIsNone(qi2.locked_by)
        self.assertIsNone(qi2.locked_since)
        self.assertEqual(qi2.priority, 1)
        self.assertEqual(qi2.escalation_level, 2)
        self.assertIsNotNone(qi.delay_release)
        self.assertGreater(qi.delay_release, datetime.datetime.now(datetime.timezone.utc))

    def test_renew(self):
        ls = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=90)
        qi = NODBQueueItem(
            queue_uuid='12345',
            priority=1,
            escalation_level=2,
            status=QueueStatus.LOCKED,
            locked_by="me",
            locked_since=ls,
            is_new=True
        )
        self.db.insert_object(qi)
        qi.renew(self.db)
        self.assertIs(qi.status, QueueStatus.LOCKED)
        self.assertEqual(qi.locked_by, 'me')
        self.assertIsNotNone(qi.locked_since)
        self.assertGreater(qi.locked_since, ls)
        self.assertEqual(qi.priority, 1)
        self.assertEqual(qi.escalation_level, 2)


    def test_release_more(self):
        qi = NODBQueueItem(
            queue_uuid='12345',
            priority=1,
            escalation_level=2,
            status=QueueStatus.LOCKED,
            locked_by="me",
            locked_since=datetime.datetime(2015, 1, 2, 3, 4, 5),
            is_new=True
        )
        self.db.insert_object(qi)
        qi.release(
            self.db,
            reduce_priority=True,
            escalation_level=5
        )
        self.assertIs(qi.status, QueueStatus.UNLOCKED)
        self.assertIsNone(qi.locked_by)
        self.assertIsNone(qi.locked_since)
        self.assertEqual(qi.priority, 2)
        self.assertEqual(qi.escalation_level, 5)
        self.assertIsNone(qi.delay_release)
        qi2 = NODBQueueItem.find_by_uuid(self.db, '12345')
        self.assertIs(qi2.status, QueueStatus.UNLOCKED)
        self.assertIsNone(qi2.locked_by)
        self.assertIsNone(qi2.locked_since)
        self.assertEqual(qi2.priority, 2)
        self.assertEqual(qi2.escalation_level, 5)
        self.assertIsNone(qi2.delay_release)
