schema_asbuilt = {
    'source_file': {
        'type': 'string',
        'required': True
    },
    'num_pages': {
        'type': 'integer',
        'required': True
    },
    'dims': {
        'type': 'list',
        'schema': {
            'page': 'integer',
            'dims': {
                'type': 'list',
                'schema': {
                    'label': {
                        'type': 'string',
                        'required': False
                    },
                    'value': {
                        'type': 'string',
                        'required': False
                    },
                    'feet': {
                        'type': 'integer',
                        'required': False
                    },
                    'inches': {
                        'type': 'integer',
                        'required': False
                    },

                }
            }
        }
    },
}


resource_asbuilt = {
    'item_title': 'asbuilts',
    'resource_methods': ['GET'],
    'schema': schema_asbuilt
}
