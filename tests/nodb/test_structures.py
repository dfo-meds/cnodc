import datetime
import unittest as ut
import cnodc.nodb.structures as structures
import zirconium as zr
from autoinject import injector

from cnodc.util import CNODCError


class UserTest(ut.TestCase):

    def test_username(self):
        user = structures.NODBUser()
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

    def test_password(self):
        user = structures.NODBUser()

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
        user = structures.NODBUser()
        self.assertEqual(len(user.modified_values), 0)
        user.assign_role('foobar')
        self.assertIn('roles', user.modified_values)
        self.assertIn('foobar', user.roles)
        user.assign_role('foobar')
        self.assertEqual(len(user.roles), 1)
        user.assign_role('foobar2')
        user.clear_modified()
        user.unassign_role('foobar')
        self.assertEqual(len(user.roles), 1)
        self.assertIn('foobar2', user.roles)
        self.assertNotIn('foobar', user.roles)
        self.assertIn('roles', user.modified_values)
        user.unassign_role('foobar')
        self.assertEqual(len(user.roles), 1)
        self.assertIn('foobar2', user.roles)
        self.assertNotIn('foobar', user.roles)
