# Note: this code is based on ebi-ait/json-converter - original code by Alexie Staffer (@MightyAx) and Károly Erdős (@ke4) 
# See: https://github.com/ebi-ait/json-converter https://pypi.org/project/json-converter/

from typing import Any, Dict, List
from controller import Transformation
import copy
from collections.abc import Mapping
import copy
import logging

logger = logging.getLogger("ETLController")

FIELD_SEPARATOR = '.'
KEYWORD_MARKER = '$'

SPEC_ANCHOR = '$on'
SPEC_FILTER = '$filter'

SPEC_OBJECT_LITERAL = '$object'
SPEC_ARRAY_LITERAL = '$array'

SPEC_ALLOWED_TRUE_VALUES = ['true', 't', 'yes', 'y']

ERROR_RETVAL_MAPPING = {
    SPEC_ARRAY_LITERAL: [],
    SPEC_OBJECT_LITERAL: {},
}


def prefix_with(*args):
    data = args[0]
    prefix = args[1]
    return f'{prefix}{data}'


# TODO make this an all-purpose date processor
def format_date(*args):
    date = args[0]
    if not date:
        return None
    return date.split('T')[0]


def concatenate_list(*args):
    items = args[0]
    if not items:
        return None
    return ' , '.join(items)


def default_to(*args):
    value = args[0]
    default_value = args[1]
    return default_value if value is None else value


def is_true(value):
    return str(value).lower() in SPEC_ALLOWED_TRUE_VALUES


def deep_set(target, key, value):
    field_chain = key.split(FIELD_SEPARATOR)
    current_node = target
    for field in field_chain[:len(field_chain) - 1]:
        if field not in current_node:
            current_node[field] = {}
        current_node = current_node[field]
    current_node[field_chain[-1]] = value


def deep_get(target: dict, key: str):
    field_chain = key.split(FIELD_SEPARATOR)
    current_node = target
    for field in field_chain:
        if current_node is None:
            return None
        current_node = current_node.get(field)
    return current_node

def check_if_valid_specification(spec) -> bool:
    if not (isinstance(spec, (list, Mapping)) and len(spec) > 0):
        logger.error(f'The specification must be a list or dict with 1 or more elements. Got {spec}')
        return False
    return True


def passes_filter(filter_spec: list, node):
    if not filter_spec:
        return True
    filter_field = filter_spec[0]
    value = node.get(filter_field)
    passing = True
    if value is not None:
        filter_args = [value]
        filter_args.extend(filter_spec[2:])
        do_filter = filter_spec[1]
        passing = bool(do_filter(*filter_args))
    return passing


def map(source, spec={}, on=''):
    if (not source or not isinstance(source, (list, Mapping))) or not check_if_valid_specification(spec):
        return {}
    
    on_field = on
    if SPEC_ANCHOR in spec:
        on_field_name = spec[SPEC_ANCHOR]
        on_field = f'{on}.{on_field_name}' if on else on_field_name
    
    node = deep_get(source, on_field) if on_field else source # change working node if needed

    filter_spec = spec.get(SPEC_FILTER)
    if not node:
        return None
    elif isinstance(node, list):
        return [step_into_node(source, list_node, on_field, spec) for list_node in node if passes_filter(filter_spec, list_node)]
    elif passes_filter(filter_spec, node):    
        return step_into_node(source, node, on_field, spec)
    else:
        return {}


def step_into_node(source, node, on_field: str, spec: dict):
    result = {} #DataNode()
    for target_field_name, field_spec in spec.items():
        # skip reserved field
        if target_field_name.startswith(KEYWORD_MARKER):
            continue
        
        check_if_valid_specification(field_spec)
        field_value = None
        if isinstance(field_spec, list):
            field_value = parse_fields_value_from_spec(source, node, field_spec)
        elif isinstance(field_spec, dict):
            field_value = map(source, spec=field_spec, on=on_field)
        deep_set(result, target_field_name, field_value)
    return result 

def parse_fields_value_from_spec(source, node, spec: list):
    if not spec:
        return None

    source_field_name = spec[0]
    
    spec_field_value = spec[1] if len(spec) > 1 else None
    
    contains_sub_spec = is_true(spec[2]) if len(spec) > 2 else False

    if source_field_name in [SPEC_ARRAY_LITERAL, SPEC_OBJECT_LITERAL]:
        if contains_sub_spec: # sub parsing is needed
            if source_field_name == SPEC_ARRAY_LITERAL:
                if spec_field_value is None or not isinstance(spec_field_value, list):
                    logger.error(f'Invalid specification: Expected list value in in pos 2 of specification: {spec}.')
                    return []
                return [map(source, spec=item, on='') for item in spec_field_value]
            
            if source_field_name == SPEC_OBJECT_LITERAL:
                if spec_field_value is None or not isinstance(spec_field_value, Mapping):
                    logger.error(f'Invalid specification: Expected some mapping in in pos 2 of specification: {spec}.')
                    return {}
                return map(source, spec=spec_field_value)
        else: # no sub parsing required, just return the thing
            if spec_field_value is None:
                return ERROR_RETVAL_MAPPING.get(source_field_name)
            return spec_field_value
    else: # either function or plain value
        source_field_value = deep_get(node, source_field_name)
        if len(spec) == 1 : # plain value
            return source_field_value
        else: # function spec
            args = [source_field_value]
            args.extend(spec[2:])
            try:
                return spec_field_value(*args)
            except Exception as e:
                logger.error(f'Error applying function {spec_field_value} to {args}: {e}')
                return None
        
class JsonConverterTransformation(Transformation):
    """Transformer that uses json-converter to transform data according to a template."""
    
    def __init__(self, params: Dict[str, Any]):
        """Initialize the JSON converter transformer.
        
        Args:
            params: Configuration parameters including:
                - specification: The specification to use for conversion
        """
        self.specification = params.get('specification', {})
        
    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform the data using the json-converter template.
        
        Args:
            data: List of dictionaries containing the data to transform
            
        Returns:
            List of dictionaries containing the transformed data
        """
        if not data:
            return []
            
        transformed_data = []
        for entry in data:
            transformed = None
            try:
                # Convert the entry according to the template
                transformed = map(entry, spec=self.specification)
                logger.debug(f"Transformed entry {entry} to {transformed}")
            except Exception as e:
                logger.error(f"Error: could not transform entry {entry}: {e}")
            transformed_data.append(transformed)
        
        return transformed_data 
    
    def initialize(self, config: Dict[str, Any]):
        pass