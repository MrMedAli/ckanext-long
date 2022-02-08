import os
import json
import collections
import resource
from unittest import result
import pandas as pd 
from pandas.io.json import json_normalize

import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit



not_empty = plugins.toolkit.get_validator('not_empty')
ignore_missing = plugins.toolkit.get_validator('ignore_missing')
ignore_empty = plugins.toolkit.get_validator('ignore_empty')




class LongPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IResourceView)
    plugins.implements(plugins.ITemplateHelpers)
    # IConfigurer



    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('fanstatic', 'long')

        

        
    def get_helpers(self):
        return {'view_data_long': _view_data_long}
        

    def info(self):
        schema = {
            'fields': [ignore_missing, ignore_empty, convert_to_string,
                       validate_fields, str],
            'varList': [ignore_missing, ignore_empty, convert_to_string,
                       validate_fields, str],
            'varName': [ignore_missing],
            'valueName': [ignore_missing],
        }

        return {'name': 'long',
                'title': 'Long Format',
                'icon': 'table',
                'iframed': False,
                'filterable': True,
                'schema': schema,
                }
    
    
    def can_view(self, data_dict):
        return True

    def view_template(self, context, data_dict):
        return 'long_view.html'

    def form_template(self, context, data_dict):
        return 'long_form.html'
        
    def setup_template_variables(self, context, data_dict):
        resource = data_dict['resource']
        fields = _get_fields_without_id(resource)

        resource_view = data_dict['resource_view']
        self._fields_as_string(resource_view)
        field_selection = json.dumps(
            [{'id': f['value'], 'text': f['value']} for f in fields]
        )

        return {'fields': fields,
                'field_selection': field_selection,
                'varList': fields,
                'varName': 'varName',
                'valueName': 'valueName',
                'allFields': fields
                }       

    def _fields_as_string(self, resource_view):
        fields = resource_view.get('fields')

        if fields:
            resource_view['fields'] = convert_to_string(fields)
    
def _view_data_long(resource_view):
    data = {
        'resource_id': resource_view['resource_id'],
        'limit': int(resource_view.get('limit', 100))
    }


    filters = resource_view.get('filters', {})
    for key, value in parse_filter_params().items():
        filters[key] = value
    data['filters'] = filters

    fields = resource_view.get('fields')
    varList = resource_view.get('varList')
    idVarsList = []
    valueVarsList = []


    result = plugins.toolkit.get_action('datastore_search')({}, data)


    if fields:
        idVarsList = convert_to_string(fields).split(',')

    if varList:
        valueVarsList = convert_to_string(varList).split(',')
    else:
        valueVarsList = get_fields_from_json(result['fields'], idVarsList)


    test = json_normalize(result['records'])
    valueName = resource_view.get('valueName')
    if valueName == '':
        valueName = 'New Value'
    varName = resource_view.get('varName')
    if varName == '':
        varName = 'New Variable'

    long_table = test.melt(id_vars = idVarsList,
    				     value_vars = valueVarsList,
    				     var_name = varName, value_name = valueName)
    long_json = long_table.to_json(orient = "split")
    parsed = json.loads(long_json)
    result['records'] = long_json
    return parsed
    
    
    
    
    
def parse_filter_params():
    filters = collections.defaultdict(list)
    filter_string = dict(plugins.toolkit.request.args).get('filters', '')
    for filter in filter_string.split('|'):
        if filter.count(':') != 1:
            continue
        key, value = filter.split(':')
        filters[key].append(value)
    return dict(filters)


def convert_to_string(value):
    if isinstance(value, list):
        return ','.join(value)
    return value


def validate_fields(key, converted_data, errors, context):
    try:
        resource = {'id': converted_data['resource_id',]}
    except KeyError:
        resource = {'id': context['resource'].id}
    value = converted_data.get(key)
    allowed_fields = set(field['id'] for field in _get_fields(resource))
    for field in value.split(','):
        if field not in allowed_fields:
            msg = 'Field {field} not in table'.format(field=field)
            raise plugins.toolkit.Invalid(msg)
    return value


def _get_fields_without_id(resource):
    fields = _get_fields(resource)
    return [{'value': v['id']} for v in fields if v['id'] != '_id']


def _get_fields(resource):
    data = {
        'resource_id': resource['id'],
        'limit': 0
    }
    result = plugins.toolkit.get_action('datastore_search')({}, data)
    return result['fields']


def get_fields_from_json (data, not_included):
    result = []
    for item in data:
        if (item['id'] != '_id' and item['id'] not in not_included):
            result.append(item['id'])
    return result


