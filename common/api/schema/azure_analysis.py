schema_azure_analysis = {
    'source_file': {
        'type': 'string',
        'required': True
    },
    'project_id': {
        'type': 'string',
        'required': True
    },
    'category': {
        'type': 'string',
        'required': True
    },
    'analysis': {
        'status': {
            'type': 'string',
            'required': True
        },
        'createdDateTime': {
            'type': 'string',
            'required': True
        },
        'analyzeResult': {}
    }
}


resource_azure_analysis = {
    'item_title': 'azure_analysis',
    'resource_methods': ['GET'],
    'schema': schema_azure_analysis
}
