# Identity Transformation
The Identity transformation is the simplest transformations possible - it sends the complete object extracted from the source to the target. is is configured in the YAML file like so:

    transformation:
        name: identity  # Use identity transformation if no data transformation needed
        params: {}  # No parameters needed for identity transformation 

# Simple Mapping

    transformation:
        name: map
        params:
            mapping:
                target_field_name: source_field_name
                some_fixed_field_name:
                    FIXED_CUSTOM_VALUE: "specified fixed value" 





# JSON Conversion

This Transformation is based on the [json-converter](https://github.com/ebi-ait/json-converter) package with some structural changes

The `json_converter` transformer is designed for translating/converting a JSON document into another JSON document with a 
different structure. The mapping process follows a dictionary-based specification of how fields map to the new 
JSON format. The main function in the `json_converter` is `map` that takes a structured specification:

        map(json_document, specification)

## Mapping Specification

The general idea is that the specification describes the resulting structure of the converted JSON document. The
dictionary-based specification will closely resemble the schema of the resulting JSON.

### Field Specification

A field specification is defined by a list of parameters, the first of which is a name that refers to a field in 
the current JSON to be converted. This is the only required field.

        <converted_field>: [<original_field>]

For example, given the sample JSON document,

        {
            "person_name": "Juan dela Cruz",
            "person_age": 37 
        }

the simplest mapping that can be done is to translate to a different field name. For example, to map 
`person_name` to `name` in the resulting JSON, the following specification is used:

JSON:
        {
            'name': ['person_name']
        }

YAML:
        name: [person_name]

#### Field Chaining

JSON mapping supports chaining of fields on either or both side of the specification. For example, using the
following specification to the JSON above,

JSON:
        {
            'person.name': ['person_name'],
            'person.age': ['person_age']
        }
 
YAML:
        person.name: [person_name]
        person.age: [person_age]

will result in the conversion:
 
        {
            "person": {
                "name": "Juan dela Cruz",
                "age": 37
            }
        }

### Anchoring

While the `json_converter` has support for field chaining, for complex JSON with several levels of nesting, 
combined with long field names and field list, repetitively providing full field chain can be tedious. To be able
to express this more concisely, anchoring can be used. Anchoring specifies the root of the JSON structure to 
map to a new JSON format, relative to the actual root of the original JSON document.

#### The `on` Parameter

The `map` function in the `json_converter` takes a parameter named `on` that can be used to specify the root of the
JSON on which to start mapping. For example:

JSON:
        map(json_object, on='address', spec={
            'address_city': ['city'],
            'address_country': ['country']
        })

YAML:
        # spec:
        address_city: [city]
        address_country: [country]
        # on: address

#### The `$on` Specification

Another way of specifying the anchoring field is by directly adding it to the specification using the `$on`
keyword. Unlike field specifications, the `$on` keyword takes a plain string and *not* a list/vector. For 
example, the previous sample specification can be alternatively expressed as,

JSON:
        {
            '$on': 'address',
            'address_city': ['city'],
            'address_country': ['country']
        }

YAML:
        $on: address
        address_city: [city]
        address_country: [country]

#### Chaining `on` and `$on`

The `on` parameter and the `$on` keyword do **not** override, but instead are chained together. The existence
of both during a mapping call results in the `$on` field chain being concatenated to the value provided in 
through the `on` parameter.

### Nested Specification

Aside from field specifications, nested dictionary-like specification can be provided to any recognised fields in
the root specification. Nesting is useful for expressing nesting on single objects, or for applying conversion
to a list of JSON objects defined in an array.

#### Single Object Nesting

For single objects, nested specs can be defined to look like the resulting JSON object. Nesting specification this
way is a more expressive alternative to [field chaining that was demonstrated above](#field-chaining). For example, 
the following JSON, similar to the previous sections,

JSON:
        {
            'profile': {
                'first_name': ['first_name'],
                'last_name': ['last_name']
            }
        }

YAML:
        profile:
          first_name: [first_name]
          last_name: [last_name]

### Array Processing

The JSON mapping utility can process arrays of objects. When it determines that a field referred to by the specification is a collection of JSON objects, it applies the rules to each one of them iteratively.

#### Array Literals

You can include array literals in the specification using the `$array` keyword:

        {
            'metadata': ['$array', ['list', 'of', 'values']],
            'empty': ['$array', []]
        }

#### Applying Specification to JSON Arrays
Specifications can be applied to specific elements of a given array, as well as to sub-elements of the elements contained in it.

For example, given a source object:

        {
            'name': 'Peter Z',
            'institution': 'Some University',
            'address': 'Some place'
        }

We can transform it into an array of key-value pairs using a complex specification:

JSON:
        {
            'attributes': ['$array', [
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
                }
            ], True]
        }

YAML:
        attributes: 
          - $array
          - - key: ['', default_to, name]
              value: [name]
            - key: ['', default_to, institution]
              value: [institution]
            - key: ['', default_to, address]
              value: [address]
          - true

This will produce:

        {
            'attributes': [
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

The third parameter `True` in the array specification indicates that the array elements should be processed as objects rather than literals.

### Object Processing

#### Json Object Literals

JSON object literals can be used directly in specifications using the `$object` operator to create fixed objects in the output. This is useful when you need to include constant data or create structured objects with predefined values.

For example, you can create a fixed object structure:

JSON:
        {
            'metadata': ['$object', {
                'type': 'test',
                'version': '1.0'
            }]
        }

YAML:
        metadata:
          - $object
          - type: test
            version: '1.0'

This will produce:

        {
            'metadata': {
                'type': 'test',
                'version': '1.0'
            }
        }

The object literal values are used as-is without any transformation. This is different from object processing where each element would be transformed according to specifications.

#### Complex specs in JSON objects

You can also create more complex nested structures using an object using the complex-spec flag (`True` as 3rd parameter):

JSON:
        {
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

YAML:
        attributes:
          - $object
          - name:
              - $object
              - first: [source.first_name]
                last: [source.last_name]
              - true
            institution:
              - $object
              - name: [source.university]
                type: [source.institution_type]
              - true
          - true

Applying this mapping to the JSON object:
        {
            "source": {
                "first_name": "Juan",
                "last_name": "dela Cruz",
                "university": "Some University",
                "institution_type": "academic"
            }
        }

Will result with the following:

        {
            'attributes': {
                'name': {
                    'first': 'Juan', 
                    'last': 'dela Cruz'
                }, 
                'institution': {
                    'name': 'Some University', 
                    'type': 'academic'
                }
            }
        }

### Error Handling

The mapper handles various error conditions gracefully:
- Missing fields return `None`
- Invalid specifications return empty objects/arrays
- Function errors return `None`
- Non-existent anchors return `None`
- Empty strings are preserved
- Non-JSON types (complex numbers, sets, bytes) are handled appropriately
- Broken $object and $array specifications return empty objects and arrays respectively

