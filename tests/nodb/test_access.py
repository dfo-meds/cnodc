import datetime
import json

from cnodc.nodb import NODBSession, NODBUser, UserStatus
from cnodc.util import CNODCError
from core import BaseTestCase

class SessionTest(BaseTestCase):

    def test_from_kwargs(self):
        session = NODBSession(
            session_id='foobar',
            start_time=datetime.datetime(2015, 1, 2, 3, 4, 5),
            expiry_time=datetime.datetime(2015, 1, 2, 4, 4, 5),
            username="foobar"
        )
        self.assertEqual(session.session_id, "foobar")
        self.assertEqual(session.start_time, datetime.datetime(2015, 1, 2, 3, 4, 5))
        self.assertEqual(session.expiry_time, datetime.datetime(2015, 1, 2, 4, 4, 5))

    def test_is_expired(self):
        session = NODBSession(
            starttime=datetime.datetime.now(datetime.timezone.utc),
            expiry_time=datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(60),
        )
        self.assertFalse(session.is_expired())

    def test_session_data(self):
        session = NODBSession()
        session.set_session_value('foo', 'bar')
        self.assertEqual(session.get_session_value('foo'), 'bar')
        self.assertIsNone(session.get_session_value('bar'))

    def test_save_load(self):
        session = NODBSession(
            session_id='foobar',
            session_data={'foo': 'bar'}
        )
        self.db.insert_object(session)
        sess2 = NODBSession.find_by_session_id(self.db, 'foobar')
        self.assertIsInstance(sess2, NODBSession)
        self.assertEqual(sess2.get_session_value('foo'), 'bar')


class UserTest(BaseTestCase):

    def test_from_kwargs(self):
        user = NODBUser(username="foo", is_new=False)
        self.assertEqual("foo", user.username)
        self.assertNotIn('username', user.modified_values)

    def test_from_kwargs_new(self):
        user = NODBUser(username="foo")
        self.assertEqual("foo", user.username)
        self.assertIn('username', user.modified_values)

    def test_username(self):
        user = NODBUser()
        self.assertEqual(len(user.modified_values), 0)
        user.username = "foobar"
        self.assertEqual(user.username, "foobar")
        self.assertEqual(len(user.modified_values), 1)
        user.username = None
        self.assertIsNone(user.username)
        self.assertEqual(len(user.modified_values), 1)
        user.clear_modified()
        self.assertEqual(len(user.modified_values), 0)
        user.username = 12345
        self.assertNotEqual(user.username, 12345)
        self.assertEqual(user.username, "12345")
        self.assertEqual(user.get_for_db("username"), "12345")

    def test_password(self):
        # speed up password hashing
        NODBUser.DEFAULT_PASSWORD_HASH_ITERATIONS = 1
        user = NODBUser()

        # Check setting the password
        self.assertIsNone(user.phash)
        self.assertIsNone(user.salt)
        user.set_password("foobar")
        self.assertIsNotNone(user.phash)
        self.assertIsNotNone(user.salt)
        self.assertIn('phash', user.modified_values)
        self.assertIn('salt', user.modified_values)

        # Good password
        self.assertTrue(user.check_password("foobar"))

        # Bad, but similar passwords
        self.assertFalse(user.check_password("Foobar"))
        self.assertFalse(user.check_password("fooba"))
        self.assertFalse(user.check_password("foobar2"))
        self.assertFalse(user.check_password("foobat"))

        # Common invalid types
        self.assertRaises(CNODCError, user.check_password, None)
        self.assertRaises(CNODCError, user.check_password, 12345)
        self.assertRaises(CNODCError, user.check_password, b'foobar')

        # Check that setting the password also changes the salt
        user.clear_modified()
        old_salt = user.salt
        user.set_password("foobar2")
        self.assertNotEqual(old_salt, user.salt)
        self.assertFalse(user.check_password("foobar"))
        self.assertTrue(user.check_password("foobar2"))
        self.assertIn('phash', user.modified_values)
        self.assertIn('salt', user.modified_values)

        # Check old password expiry
        user.clear_modified()
        user.set_password("foobar3", old_expiry_seconds=60)
        self.assertTrue(user.check_password("foobar3"))
        self.assertTrue(user.check_password("foobar2"))
        self.assertFalse(user.check_password("foobar"))
        user.old_expiry = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=60)
        self.assertFalse(user.check_password("foobar2"))
        self.assertTrue(user.check_password("foobar3"))
        self.assertIn('old_expiry', user.modified_values)
        self.assertIn('old_phash', user.modified_values)
        self.assertIn('old_salt', user.modified_values)

        # Check cleaning up old password
        self.assertIsNotNone(user.old_expiry)
        self.assertIsNotNone(user.old_phash)
        self.assertIsNotNone(user.old_salt)
        user.clear_modified()
        user.cleanup()
        self.assertIsNone(user.old_expiry)
        self.assertIsNone(user.old_phash)
        self.assertIsNone(user.old_salt)
        self.assertIn('old_expiry', user.modified_values)
        self.assertIn('old_phash', user.modified_values)
        self.assertIn('old_salt', user.modified_values)

        # Check some bad passwords
        self.assertRaises(CNODCError, user.set_password, None)
        self.assertRaises(CNODCError, user.set_password, 12345)
        self.assertRaises(CNODCError, user.set_password, 'x' * 1025)
        self.assertRaises(CNODCError, user.set_password, '')

    def test_roles(self):
        user = NODBUser()
        self.assertEqual(len(user.modified_values), 0)
        user.assign_role('foobar')
        self.assertIn('roles', user.modified_values)
        self.assertIn('foobar', user.roles)
        user.assign_role('foobar')
        self.assertEqual(len(user.roles), 1)
        user.assign_role('foobar2')
        user.clear_modified()
        self.assertEqual(["foobar", "foobar2"], json.loads(user.get_for_db("roles")))
        user.unassign_role('foobar')
        self.assertEqual(len(user.roles), 1)
        self.assertIn('foobar2', user.roles)
        self.assertNotIn('foobar', user.roles)
        self.assertIn('roles', user.modified_values)
        user.unassign_role('foobar')
        self.assertEqual(len(user.roles), 1)
        self.assertIn('foobar2', user.roles)
        self.assertNotIn('foobar', user.roles)

    def test_status(self):
        user = NODBUser()
        user.status = UserStatus.ACTIVE
        self.assertIs(user.status, UserStatus.ACTIVE)
        self.assertEqual(UserStatus.ACTIVE.value, user.get_for_db("status"))

    def test_load_permissions(self):
        self.db.grant_permission('foo', 'bar')
        self.db.grant_permission('foo2', 'bar2')
        self.db.grant_permission('foo3', 'bar3')
        user = NODBUser()
        user.assign_role('foo')
        user.assign_role('foo2')
        self.assertIn('foo', user.roles)
        self.assertIn('foo2', user.roles)
        self.assertNotIn('foo3', user.roles)
        perms = user.permissions(self.db)
        self.assertIn('permissions', user._cache)
        self.assertEqual(2, len(perms))
        self.assertIn('bar', perms)
        self.assertIn('bar2', perms)
        self.assertNotIn('bar3', perms)
        user.unassign_role('foo2')
        perms2 = user.permissions(self.db)
        self.assertEqual(1, len(perms2))
        self.assertIn('bar', perms2)
        self.assertNotIn('bar2', perms2)
        self.assertNotIn('bar3', perms2)

    def test_find_all(self):
        self.db.insert_object(NODBUser(username="foo"))
        self.db.insert_object(NODBUser(username="foo2"))
        users = [x.username for x in NODBUser.find_all(self.db)]
        self.assertIn('foo', users)
        self.assertIn('foo2', users)

    def test_by_username(self):
        self.db.insert_object(NODBUser(username="foo"))
        self.db.insert_object(NODBUser(username="foo2"))
        foo = NODBUser.find_by_username(self.db, 'foo')
        self.assertIsNotNone(foo)
        self.assertIsInstance(foo, NODBUser)
        self.assertEqual(foo.username, 'foo')

    def test_missing_by_username(self):
        self.db.insert_object(NODBUser(username="foo"))
        self.db.insert_object(NODBUser(username="foo2"))
        foo3 = NODBUser.find_by_username(self.db, 'foo3')
        self.assertIsNone(foo3)
