import pytest
from unittest.mock import patch
import json
from string import Template

from transformers.json_converter import (
    JsonConverterTransformation,  
    default_to,
    prefix_with,
    format_date,
    concatenate_list,
    deep_get,
    deep_set,
)
from transformers.json_converter import map as json_map

def test_map_object():
    # given:
    json_object = json.loads('''{
        "user_name": "jdelacruz",
        "user_age": 31  
    }''')

    # when:
    converted_json = json_map(json_object,{
        'name': ['user_name'],
        'age': ['user_age']
    })

    # then:
    assert converted_json is not None
    assert 'jdelacruz' == converted_json['name']
    assert 31 ==converted_json['age']

def test_map_object_using_field_chaining():
    # given:
    json_object = json.loads('''{
        "name": "John Doe",
        "address": {
            "city": "London",
            "country": "UK"
        }
    }''')

    # when:
    profile_json = json_map(json_object,{
        'user.profile': ['name'],
        'user.address_city': ['address.city'],
        'user.address_country': ['address.country']
    })

    print(profile_json)
    # then:
    assert profile_json is not None
    assert 'John Doe' == profile_json.get('user', {}).get('profile')
    assert 'London' == profile_json.get('user', {}).get('address_city')
    assert 'UK' == profile_json.get('user', {}).get('address_country')

def test_map_object_with_anchored_key():
    # given:
    json_object = json.loads('''{
        "user": {
            "name": "Jane Doe"
        },
        "address": {
            "city": "Cambridge",
            "country": "UK"
        }
    }''')

    # when:
    address_json = json_map(json_object,
        on='address',
        spec={
            'address_city': ['city'],
            'address_country': ['country']
        })

    # then:
    assert address_json is not None
    assert 'Cambridge' == address_json['address_city']
    assert 'UK', address_json['address_country']

def test_map_object_using_spec_based_anchor():
    # given:
    json_object = json.loads('''{
        "shipping_info": {
            "recipient": "Kamado Tanjiro",
            "address": "Tokyo, Japan"
        }
    }''')

    # when:
    delivery_json = json_map(json_object,{
        '$on': 'shipping_info',
        'name': ['recipient'],
        'location': ['address']
    })

    # then:
    assert 'Kamado Tanjiro' == delivery_json.get('name')
    assert 'Tokyo, Japan' == delivery_json.get('location')

def test_map_object_using_chained_spec_based_anchors():
    # given:
    json_object = json.loads('''{
        "user": {
            "profile": {
                "name": "Kamado Nezuko",
                "location": "hako no naka"
            }
        }
    }''')

    # when:
    person_json = json_map(json_object,{
        '$on': 'user',
        'person': {
            '$on': 'profile',
            'known_by': ['name']
        }
    })

    # then:
    assert 'Kamado Nezuko'  == person_json.get('person', {}).get('known_by')

def test_map_object_with_non_node_anchor_parameter():
    # given:
    json_object = json.loads('''{
        "name": "Boaty McBoatface"
    }''')

    # expect:
    result = json_map(json_object,{
        'known_as': ['name']
    }, on='non_existent_node')
    assert result is None

def test_map_object_with_non_existent_anchor():
    # given:
    json_object = json.loads('''{
        "description": "test"
    }''')

    # when:
    flat_result = json_map(json_object,{
        '$on': 'non.existent.node',
        'name': ['text']
    })

    # and:
    nested_result = json_map(json_object,{
        'text': ['description'],
        'optional': {
            '$on': 'non.existent.node',
            'field': ['field']
        }
    })

    # then:
    assert flat_result is None
    assert {'text': 'test', 'optional': None} == nested_result

def test_map_object_with_custom_processing():
    # given:
    json_object = json.loads('''{
        "name": "Pedro,Catapang,de Guzman",
        "age": 44
    }''')

    # and:
    def parse_name(*args):
        name = args[0]
        index = args[1]
        return name.split(',')[index]

    # and:
    def fake_age(*args):
        age = args[0]
        return age - 10

    # when:
    resulting_json = json_map(json_object,{
        'first_name': ['name', parse_name, 0],
        'middle_name': ['name', parse_name, 1],
        'last_name': ['name', parse_name, 2],
        'fake_age': ['age', fake_age]
    })

    # then:
    assert 'Pedro' == resulting_json['first_name']
    assert 'Catapang' == resulting_json['middle_name']
    assert 'de Guzman' == resulting_json['last_name']
    assert 34 == resulting_json['fake_age']

def test_map_object_with_nested_spec():
    # given:
    json_object = json.loads('''{
        "first_name": "Vanessa",
        "last_name": "Doofenshmirtz"
    }''')

    # when:
    profile_json = json_map(json_object,{
        'profile': {
            'first_name': ['first_name'],
            'last_name': ['last_name']
        }
    })

    # then:
    profile = profile_json.get('profile')
    assert profile is not None
    assert 'Vanessa' == profile.get('first_name')
    assert 'Doofenshmirtz' == profile.get('last_name')

def test_map_list_of_objects():
    # given:
    json_object = json.loads('''{
        "contacts": [
            {
                "name": "James",
                "phone": "55556161"
            },
            {
                "name": "Ana",
                "phone": "55510103"
            }
        ]
    }''')

    # when:
    people = json_map(json_object,on='contacts', spec={
        'known_by': ['name']
    })

    # then:
    assert 2 == len(people)
    names = [person.get('known_by') for person in people]
    assert 'Ana' in names
    assert 'James' in names

def test_map_list_of_objects_with_nested_anchoring():
    # given:
    json_object = json.loads('''{
        "social_network": {
            "friends": [
                {
                    "name": "Carl",
                    "age": 24
                },
                {
                    "name": "Tina",
                    "age": 22
                },
                {
                    "name": "Oscar",
                    "age": 22
                }
            ]
        }
    }''')

    # when:
    people_json = json_map(json_object,{
        '$on': 'social_network',
        'people': {
            '$on': 'friends',
            'person_name': ['name'],
            'person_age': ['age']
        }
    })

    # then:
    people = people_json.get('people')
    assert people is not None
    assert 3 == len(people)


def test_map_object_with_complex_object():
    # given:
    json_object = json.loads('''{
        "source": {
            "first_name": "Juan",
            "last_name": "dela Cruz",
            "university": "Some University",
            "institution_type": "academic"
            }
    }''')
    
    spec = {
        'attributes': ['$object', {
            'name': ['$object', {
                'first': ['source.first_name'],
                'last': ['source.last_name']
            }, True],
            'institution': ['$object', {
                'name': ['source.university'],
                'type': ['source.institution_type']
            }, True]
        }, True]
    }

    # when:
    result = json_map(json_object, spec)

    # then:
    print(result)
    assert result.get('attributes') is not None
    assert result.get('attributes').get('name').get('first') == 'Juan'
    assert result.get('attributes').get('name').get('last') == 'dela Cruz'
    assert result.get('attributes').get('institution').get('name') == 'Some University'
    assert result.get('attributes').get('institution').get('type') == 'academic'

# TODO consider required field mode
def test_map_object_ignore_missing_fields():
    # given:
    json_object = json.loads('''{
        "first_name": "Juan",
        "last_name": "dela Cruz"
    }''')

    # when:
    person_json = json_map(json_object,{
        'fname': ['first_name'],
        'mname': ['middle_name'],
        'lname': ['last_name']
    })

    # then:
    assert 'Juan'  == person_json.get('fname')
    assert 'dela Cruz' == person_json.get('lname')
    assert 'mname' in person_json
    assert person_json.get('mname') is None

def test_map_object_allow_empty_strings():
    # given:
    json_object = json.loads('''{
        "pet_name": "Champ",
        "favourite_food": ""
    }''')

    # when:
    pet_json = json_map(json_object,{
        'name': ['pet_name'],
        'fav_food': ['favourite_food']
    })

    # then:
    assert 'Champ' == pet_json.get('name')
    assert ''  == pet_json.get('fav_food')

def test_map_object_with_invalid_spec():
    # given:
    json_object = json.loads('''{
        "description": "this is a test"
    }''')

    # expect: main spec
    result = json_map(json_object, spec='non-sense spec')
    assert result == {}

    # expect: object literal
    result = json_map(json_object,{
        'field': []
    })
    assert 'field' in result
    assert result.get('field') is None

    # and: field spec
    result = json_map(json_object,{'d': 'specification'})
    assert 'd' in result
    assert result.get('d') is None

    # and: empty dict as spec
    result = json_map(json_object,{})
    assert result == {}

    # and: empty field specification
    result = json_map(json_object,{'field': []})
    assert 'field' in result
    assert result.get('field') is None

    # and: None field specification
    result = json_map(json_object,{'field': None})
    assert 'field' in result
    assert result.get('field') is None

def test_map_object_with_filter():
    # given:
    json_template = Template('''{
        "name": "$name",
        "age": $age
    }''')

    # and:
    filtered_out_json = json.loads(json_template.substitute({'name': 'Charlie', 'age': 10}))
    passing_json = json.loads(json_template.substitute({'name': 'Mary', 'age': 27}))

    # and:
    def is_adult(*args):
        age = args[0]
        return age >= 18

    # and:
    filtered_spec = {
        '$filter': ['age', is_adult],
        'known_by': ['name']
    }

    # when:
    under_age_result = json_map(filtered_out_json, filtered_spec)
    adult_result = json_map(passing_json, filtered_spec)

    # then:
    assert {} == under_age_result
    assert {'known_by': 'Mary'} == adult_result

def test_map_object_list_with_filter():
    # given:
    json_object = json.loads('''{
        "product_list": [
            {
                "name": "eggs",
                "price": 1.25
            },
            {
                "name": "milk",
                "price": 0.50
            },
            {
                "name": "loaf",
                "price": 2.25
            }
        ]
    }''')

    # and:
    def price_filter(*args):
        price = args[0]
        return price >= 1

    # when:
    products = json_map(json_object,{
        '$on': 'product_list',
        '$filter': ['price', price_filter],
        'item': ['name']
    })

    # then:
    assert 2 == len(products)
    item_names = [item.get('item') for item in products]
    assert 'eggs' in item_names and 'loaf' in item_names

def test_map_with_object_literal():
    # given:
    json_object = json.loads('''{
        "description": "test"
    }''')

    # when:
    metadata = {'authored_by': 'me'}
    result = json_map(json_object,{
        'text': ['description'],
        'metadata': ['$object', metadata],
        'empty': ['$object', {}]
    })

    # then:
    assert metadata == result.get('metadata')
    assert 'empty' in result
    assert result.get('empty') == {} # the default for missing object values

def test_map_with_object_with_spec():
    # given:
    json_dict = {
        'source_key': 'source_value'
    }

    # when:
    values = {
        'obj_with_spec': {
            'new_key': ['source_key']
        }
    }

    result = json_map(json_dict,{
        'metadata': ['$object', values, True],
        'empty': ['$object', {}]
    })

    expected_value = {
        'obj_with_spec': {
            'new_key': 'source_value'
        }
    }

    # then:
    assert expected_value == result.get('metadata')
    assert 'empty' in result
    assert result.get('empty') == {}

def test_map_with_invalid_object_literal():
    # given:
    json_dict = {
        "source_key": "source_value"
    }

    # expect: no specified literal
    result = json_map(json_dict, {
        'field': ['$object'],
        'field2': ['$object', None, True],
        'target_key': ['source_key']
    })
    assert 'field' in result
    assert result.get('field') == {}
    assert result.get('field2') == {}
    assert 'target_key' in result
    assert 'source_value' == result.get('target_key')


def test_map_with_invalid_array_literal():
    # given:
    json_dict = {
        "source_key": "source_value"
    }

    # expect: no specified literal
    result = json_map(json_dict, {
        'field': ['$array'],
        'field2': ['$array', None, True],
        'target_key': ['source_key']
    })
    assert 'field' in result
    assert result.get('field') == []
    assert result.get('field2') == []
    assert 'target_key' in result
    assert 'source_value' == result.get('target_key')


def test_map_with_error_thrown_from_function_spec():
    # given:
    json_dict = {
        "source_key": "source_value"
    }
    
    def error_function(*args):
        raise Exception("Test error")
    
    # when:
    result = json_map(json_dict, {
        'field': ['source_key', error_function]
    })

    # then:
    assert 'field' in result
    assert result.get('field') is None

def test_map_with_default_to():
    # given:
    source_object = {
        'name': 'Peter Z',
        'institution': 'Some University',
        'address': 'Some place'
    }

    # when:
    values = [
        {
            'key': ['', default_to, 'name'],
            'value': ['name']

        },
        {
            'key': ['', default_to, 'institution'],
            'value': ['institution']

        },
        {
            'key': ['', default_to, 'address'],
            'value': ['address']

        },
    ]

    result = json_map(source_object, {
        'attributes': ['$array', values, True]
    })


    expected_result = {
        'attributes':[
            {
                'key': 'name',
                'value': 'Peter Z'
            },
            {
                'key': 'institution',
                'value': 'Some University'
            },
            {
                'key': 'address',
                'value': 'Some place'
            }
        ]
    }

    assert expected_result == result
    

def test_map_with_prefix_with():
    # given:
    source_object = {
        'name': 'Peter Z',
        'institution': 'Some University',
        'address': 'Some place'
    }

    result = json_map(source_object, {
        'target_name': ['name', prefix_with, 'Mr. ']
    })

    assert 'target_name' in result
    assert result.get('target_name') == 'Mr. Peter Z'   

    # when:
    values = [
        {
            'target_name': ['name', prefix_with, 'Mr. ']

        },
        {
            'target_institution': ['institution', prefix_with, 'of-']

        },
        {
            'target_address': ['address', prefix_with, 'Located at ']

        },
    ]

    result = json_map(source_object, {
        'attributes': ['$array', values, True]
    })


    expected_result = {
        'attributes':[
            {
                'target_name': 'Mr. Peter Z'
            },
            {
                'target_institution': 'of-Some University'
            },
            {
                'target_address': 'Located at Some place'
            }
        ]
    }

    assert expected_result == result


def test_map_with_array_literal():
    # given:
    json_dict = {"data": "test"}

    # when:
    values = ['list', 'of', 'values']
    result = json_map(json_dict, {
        'metadata': ['$array', values],
        'empty': ['$array', []]
    })

    # then:
    assert "data" not in result
    assert values == result.get('metadata')
    assert 'empty' in result
    assert result.get('empty') == [] # the default for array literal

def test_map_with_array_object_with_spec():
    # given:
    json_dict = {
        'from_key': 'from_value'
    }

    # when:
    values = [
        {
            'name': ['', default_to, 'name'],
            'value': ['from_key']
        }
    ]
    result = json_map(json_dict, {
        'metadata': ['$array', values, True],
        'empty': ['$array', []]
    })

    expected_value = [
        {
            'name': 'name',
            'value': 'from_value'
        }
    ]

    # then:
    assert expected_value == result.get('metadata')
    assert 'empty' in result
    assert result.get('empty') == []

def test_prefix_with():
    # given:
    data = "test"
    prefix = "pre_"
    
    # when:
    result = prefix_with(data, prefix)
    
    # then:
    assert result == "pre_test"
    assert prefix_with("", "pre_") == "pre_"

def test_format_date():
    # given:
    date_with_time = "2023-01-01T12:00:00"
    empty_date = ""
    none_date = None
    
    # when:
    result_with_time = format_date(date_with_time)
    result_empty = format_date(empty_date)
    result_none = format_date(none_date)
    
    # then:
    assert result_with_time == "2023-01-01"
    assert result_empty is None
    assert result_none is None

def test_concatenate_list():
    # given:
    items = ["a", "b", "c"]
    empty_items = []
    none_items = None
    
    # when:
    result = concatenate_list(items)
    result_empty = concatenate_list(empty_items)
    result_none = concatenate_list(none_items)
    
    # then:
    assert result == "a , b , c"
    assert result_empty is None
    assert result_none is None

def test_deep_get():
    # given:
    data = {
        "level1": {
            "level2": {
                "level3": "value"
            }
        }
    }
    
    # when/then:
    assert deep_get(data, "level1.level2.level3") == "value"
    assert deep_get(data, "level1.level2") == {"level3": "value"}
    assert deep_get(data, "level1") == {"level2": {"level3": "value"}}
    assert deep_get(data, "nonexistent") is None
    assert deep_get(data, "level1.nonexistent") is None
    assert deep_get(data, "level1.level2.nonexistent") is None

def test_deep_set():
    # given:
    data = {}
    
    # when:
    deep_set(data, "level1.level2.level3", "value")
    
    # then:
    assert data == {
        "level1": {
            "level2": {
                "level3": "value"
            }
        }
    }
    
    # when: setting value at existing path
    deep_set(data, "level1.level2.level3", "new_value")
    
    # then:
    assert data["level1"]["level2"]["level3"] == "new_value"
    
    # when: setting value at new path
    deep_set(data, "level1.level2.new_field", "another_value")
    
    # then:
    assert data["level1"]["level2"]["new_field"] == "another_value"



# def test_parse_complex_field_invalid_type():
#     # given:
#     source = {}
#     invalid_object = object()
#     spec = ["$object", invalid_object, True]
    
#     # expect:
#     result = parse_complex_field(source, spec)
#     assert result == str(invalid_object)

# def test_parse_complex_field_invalid_spec():
#     # given:
#     source = {}
#     spec = ["$object", ["not_a_dict_or_list"], True]
    
#     # expect:
#     with pytest.raises(UnreadableSpecification):
#         parse_complex_field(source, spec)

def test_deep_nested_structure():
    # given:
    data = {}
    current = data
    for i in range(100):
        current["nested"] = {}
        current = current["nested"]
    
    spec = {"value": ["nested"] * 100}
    
    # when:
    result = json_map(data, spec)
    
    # then:
    assert result is not None

def test_non_json_types():
    # given:
    data = {
        "complex": complex(1, 2),
        "set": {1, 2, 3},
        "bytes": b"test"
    }
    spec = {
        "c": ["complex"],
        "s": ["set"],
        "b": ["bytes"]
    }
    
    # when:
    result = json_map(data, spec)
    
    # then:
    assert result is not None

def test_initialize_method():
    # given:
    config = {"some_config": "value"}
    transformer = JsonConverterTransformation({"specification": {}})
    
    # when:
    transformer.initialize(config)
    
    # then:
    # No assertions needed as initialize is a no-op, but we verify it doesn't raise exceptions

@pytest.fixture
def sample_data():
    return [
        {
            "user": {
                "id": 1,
                "name": "John Doe",
                "email": "john@example.com",
                "address": {
                    "street": "123 Main St",
                    "city": "Boston",
                    "state": "MA"
                }
            }
        }
    ]

@pytest.fixture
def specification():
    return {
        "user_id": ["user.id"],
        "full_name": ["user.name"],
        "contact": {
            "email": ["user.email"],
            "location": {
                "city": ["user.address.city"],
                "state": ["user.address.state"],
            }
        }
    }

@pytest.fixture
def transformer(specification):
    return JsonConverterTransformation({
        "specification": specification,
    })

def test_initialization():
    """Test transformer initialization with parameters."""
    specification = {"field": "value"}
    
    transformer = JsonConverterTransformation({
        "specification": specification,
    })
    
    assert transformer.specification == specification

def test_transform_empty_data(transformer):
    """Test transformation with empty data."""
    result = transformer.transform([])
    assert result == []

def test_transform_data(transformer, sample_data):
    """Test successful data transformation."""
    result = transformer.transform(sample_data)
    
    assert len(result) == 1
    transformed = result[0]
    
    assert transformed["user_id"] == 1
    assert transformed["full_name"] == "John Doe"
    assert transformed["contact"]["email"] == "john@example.com"
    assert transformed["contact"]["location"]["city"] == "Boston"
    assert transformed["contact"]["location"]["state"] == "MA"

def test_transform_error_handling(transformer):
    """Test error handling during transformation."""
    invalid_data = [{"invalid": "data"}]

    with patch('transformers.json_converter.map', side_effect=Exception("Test error")):
        retval = transformer.transform(invalid_data)
        assert len(retval) == 1
        assert retval[0] is None


def test_transform_partial_error_handling(transformer, sample_data):
    """Test error handling during transformation."""
    entry_count = 5
    broken_entry_index = 3
    test_data = [ {'some': 'data', "index": (i+1)} for i in range(entry_count)]
    print("XXX", test_data)


    call_count = 0
    
    def failing_map(entries, spec={}, on=''):
        nonlocal call_count
        call_count += 1
        if call_count-1 == broken_entry_index:
            raise Exception("Test error")
        return {"valid": "data"}

    with patch('transformers.json_converter.map', failing_map): #TODO: fix me
        retval = transformer.transform(test_data)
        assert len(retval) == len(test_data)
        assert retval[broken_entry_index] is None
        for i, r in enumerate(retval):
            if i != broken_entry_index:
                assert r is not None


def test_nested_specification_transformation():
    """Test transformation with deeply nested specification."""
    specification = {
        '$on': 'social_network',
        'people': {
            '$on': 'friends',
            'person_name': ['name'],
            'person_age': ['age']
        }
    }
    
    transformer = JsonConverterTransformation({"specification": specification})
    data = [{
        "social_network": {
            "friends": [
                {
                    "name": "Carl",
                    "age": 24
                },
                {
                    "name": "Tina",
                    "age": 22
                },
                {
                    "name": "Oscar",
                    "age": 22
                }
            ]
        }
    }]
    
    result = transformer.transform(data)
    assert len(result) == 1
    transformed = result[0]
    
    friends = data[0]["social_network"]["friends"]
    assert "people" in transformed
    people = transformed.get('people')
    assert people is not None
    assert len(friends) == len(people)

    
    found_friends = 0
    for friend in friends:
        found = False
        for person in people:
            if person["person_name"] == friend["name"]:
                assert not found
                found = True
                assert person["person_age"] == friend["age"]
                found_friends += 1
        assert found

    assert found_friends == len(friends)
