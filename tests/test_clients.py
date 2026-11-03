import sqlite3
import tempfile
import unittest
from pathlib import Path
from common.clients import client_identity,open_registry,ensure_clients

class ClientRegistryTest(unittest.TestCase):
    def test_integer_keys_stable_and_names_do_not_merge_addresses(self):
        first=("Casey","Taylor","10 Main","Town","NY","00123")
        second=("Casey","Taylor","11 Main","Town","NY","00123")
        with tempfile.TemporaryDirectory() as directory:
            connection=open_registry(Path(directory)/"clients.sqlite")
            before=ensure_clients(connection,{first,second})
            after=ensure_clients(connection,[second,first])
            self.assertEqual(before,after)
            self.assertNotEqual(before[first],before[second])
            self.assertTrue(all(type(value) is int and value>0 for value in before.values()))
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("INSERT INTO clients(name_1,name_2,street,one_city,us_state,post_code) VALUES(?,?,?,?,?,?)",first)
            connection.rollback()
            connection.close()
            connection=open_registry(Path(directory)/"clients.sqlite")
            self.assertEqual(ensure_clients(connection,[first,second]),before)
            connection.close()

    def test_new_clients_do_not_renumber_existing_keys(self):
        original=("Zoe","Taylor","10 Main","Town","NY","00123")
        newer=("Amy","Taylor","11 Main","Town","DC","00001")
        with tempfile.TemporaryDirectory() as directory:
            connection=open_registry(Path(directory)/"clients.sqlite")
            first=ensure_clients(connection,[original])[original]
            mapping=ensure_clients(connection,[newer,original])
            self.assertEqual(mapping[original],first)
            self.assertGreater(mapping[newer],first)
            connection.close()
