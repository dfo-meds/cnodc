from tests.helpers.base_test_case import BaseTestCase
from medsutil import json

class TestJson(BaseTestCase):

    def test_load_dict_success(self):
        x = {"foo": "bar"}
        y = json.load_dict(json.dumps(x))
        self.assertEqual(x, y)

    def test_load_dict_failure(self):
        x = ["foo", "bar"]
        with self.assertRaises(TypeError):
            json.load_dict(json.dumps(x))

    def test_load_list_success(self):
        x = ["foo", "bar"]
        self.assertEqual(json.load_list(json.dumps(x)), x)

    def test_load_list_failure(self):
        x = {"foo": "bar"}
        with self.assertRaises(TypeError):
            json.load_list(json.dumps(x))

    def test_load_set_success(self):
        x = {"foo", "bar"}
        self.assertEqual(json.load_set(json.dumps(x)), x)

    def test_load_set_failure(self):
        x = {"foo": "bar"}
        with self.assertRaises(TypeError):
            json.load_set(json.dumps(x))

    def test_clean_uuid(self):
        import uuid
        x = uuid.uuid4()
        self.assertEqual(str(x), json.clean_for_json(x))

    def test_clean_raises_type_error(self):
        with self.assertRaises(TypeError):
            json.clean_for_json(self)

    def test_clean_for_orjson(self):
        from orjson import JSONEncodeError
        with self.assertRaises(JSONEncodeError):
            json._clean_for_orjson(self)

