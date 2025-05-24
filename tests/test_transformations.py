import unittest
from typing import Dict, Any
from transformations.identity import IdentityTransformation
from transformations.map import MapTransformation
from controller import MISSING_DATA_DEFAULT_VALUE, FIXED_CUSTOM_VALUE_KEY

class TestIdentityTransformation(unittest.TestCase):
    def setUp(self):
        self.transformation = IdentityTransformation()
        self.transformation.initialize({})

    def test_identity_transformation(self):
        test_data = {
            "field1": "value1",
            "field2": 123,
            "field3": {"nested": "value"},
            "field4": [1, 2, 3]
        }
        result = self.transformation.transform(test_data)
        self.assertEqual(result, test_data)
        self.assertIsNot(result, test_data)  # Should be a new dict, not the same object

class TestMapTransformation(unittest.TestCase):
    def setUp(self):
        self.transformation = MapTransformation()

    def test_direct_field_mapping(self):
        config = {
            "mapping": {
                "target_field1": "source_field1",
                "target_field2": "source_field2"
            }
        }
        self.transformation.initialize(config)

        test_data = {
            "source_field1": "value1",
            "source_field2": "value2",
            "source_field3": "value3"  # Should be ignored
        }

        expected = {
            "target_field1": "value1",
            "target_field2": "value2"
        }

        result = self.transformation.transform(test_data)
        self.assertEqual(result, expected)

    def test_missing_field_mapping(self):
        config = {
            "mapping": {
                "target_field1": "source_field1",
                "target_field2": "missing_field"
            }
        }
        self.transformation.initialize(config)

        test_data = {
            "source_field1": "value1"
        }

        expected = {
            "target_field1": "value1",
            "target_field2": MISSING_DATA_DEFAULT_VALUE
        }

        result = self.transformation.transform(test_data)
        self.assertEqual(result, expected)

    def test_list_mapping(self):
        config = {
            "mapping": {
                "target_field": ["source_field1", "source_field2", "missing_field"]
            }
        }
        self.transformation.initialize(config)

        test_data = {
            "source_field1": "value1",
            "source_field2": "value2"
        }

        expected = {
            "target_field": ["value1", "value2", MISSING_DATA_DEFAULT_VALUE]
        }

        result = self.transformation.transform(test_data)
        self.assertEqual(result, expected)

    def test_dictionary_mapping(self):
        config = {
            "mapping": {
                "target_field": {
                    "key1": "source_field1",
                    "key2": "source_field2",
                    "key3": "missing_field"
                }
            }
        }
        self.transformation.initialize(config)

        test_data = {
            "source_field1": "value1",
            "source_field2": "value2"
        }

        expected = {
            "target_field": {
                "key1": "value1",
                "key2": "value2",
                "key3": MISSING_DATA_DEFAULT_VALUE
            }
        }

        result = self.transformation.transform(test_data)
        self.assertEqual(result, expected)

    def test_constant_value_mapping(self):
        config = {
            "mapping": {
                "target_field": {
                    FIXED_CUSTOM_VALUE_KEY: "constant value"
                }
            }
        }
        self.transformation.initialize(config)

        test_data = {
            "any_field": "any_value"  # Should be ignored
        }

        expected = {
            "target_field": "constant value"
        }

        result = self.transformation.transform(test_data)
        self.assertEqual(result, expected)

    def test_empty_mapping(self):
        config = {
            "mapping": {}
        }
        self.transformation.initialize(config)

        test_data = {
            "field1": "value1",
            "field2": "value2"
        }

        expected = {}

        result = self.transformation.transform(test_data)
        self.assertEqual(result, expected)

    def test_complex_mapping(self):
        config = {
            "mapping": {
                "target_field1": "source_field1",
                "target_field2": ["source_field2", "source_field3"],
                "target_field3": {
                    "nested_key1": "source_field4",
                    "nested_key2": "missing_field"
                },
                "target_field4": {
                    FIXED_CUSTOM_VALUE_KEY: "constant"
                }
            }
        }
        self.transformation.initialize(config)

        test_data = {
            "source_field1": "value1",
            "source_field2": "value2",
            "source_field3": "value3",
            "source_field4": "value4"
        }

        expected = {
            "target_field1": "value1",
            "target_field2": ["value2", "value3"],
            "target_field3": {
                "nested_key1": "value4",
                "nested_key2": MISSING_DATA_DEFAULT_VALUE
            },
            "target_field4": "constant"
        }

        result = self.transformation.transform(test_data)
        self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main() 