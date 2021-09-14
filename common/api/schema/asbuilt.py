schema_asbuilt = {
    'source_file': {
        'type': 'string',
        'required': True
    },
    'num_pages': {
        'type': 'integer',
        'required': True
    },
    'pages': {
        'type': 'list',
        'schema': {
            'pdf': { 'type': 'string', 'required': True },
            'image': {'type': 'string', 'required': True},
            'image_width': {'type': 'integer', 'required': True},
            'image_height': {'type': 'integer', 'required': True},
            'red_image': {'type': 'string', 'required': True},
            'has_red_pixels': {'type': 'bool', 'required': True},
            'page': {'type': 'integer', 'required': True},
            'raw_text': {'type': 'string', 'required': False},
            'ocr_analysis_id': {'type': 'string', 'required': False},
            'red_ocr_analysis_id': {'type': 'string', 'required': False}
        }
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
