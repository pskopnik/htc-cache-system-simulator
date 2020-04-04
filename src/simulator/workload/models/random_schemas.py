

base_schema = """
{
    "$schema": "http://json-schema.org/draft-07/schema",
    "$id": "https://simulator/workload/models/random_parameters_schema.json",
    "type": "object",
    "required": [
        "data_set",
        "no_of_tasks",
        "read_fraction",
        "submit_rate"
    ],
    "properties": {
        "data_set": {
            "$id": "#data_set",
            "type": "object",
            "required": [
                "size",
                "file_size"
            ],
            "properties": {
                "size": {
                    "$ref": "#/$defs/bytes_size"
                },
                "file_size": {
                    "$ref": "#/$defs/bytes_size"
                }
            },
            "additionalProperties": false
        },
        "no_of_tasks": {
            "$id": "#no_of_tasks",
            "type": "integer",
            "minimum": 0
        },
        "read_fraction": {
            "$ref": "#/$defs/fraction"
        },
        "submit_rate": {
            "$ref": "#/$defs/bytes_rate"
        }
    },
    "additionalProperties": false,
    "$defs": {
        "bytes_size": {
            "type": "string",
            "pattern": "^\\d+(\\.\\d+)? ([KMGTPEZY]?i)?B$"
        },
        "bytes_rate": {
            "type": "string",
            "pattern": "^\\d+(\\.\\d+)? ([KMGTPEZY]?i)?B\\/s$"
        },
        "fraction": {
            "type": "number",
            "minimum": 0.0,
            "maximum": 1.0
        }
    }
}
"""

union_type_schema = """
{
    "$schema": "http://json-schema.org/draft-07/schema",
    "$id": "https://simulator/workload/models/random_parameters_schema.json",
    "type": "object",
    "required": [
        "data_set",
        "no_of_tasks",
        "read_fraction",
        "submit_rate"
    ],
    "properties": {
        "data_set": {
            "$id": "#data_set",
            "type": "object",
            "required": [
                "size",
                "file_size"
            ],
            "properties": {
                "size": {
                    "$ref": "#/$defs/union_bytes_size"
                },
                "file_size": {
                    "$ref": "#/$defs/union_bytes_size"
                }
            },
            "additionalProperties": false
        },
        "no_of_tasks": {
            "$id": "#no_of_tasks",
            "type": "integer",
            "minimum": 0
        },
        "read_fraction": {
            "$ref": "#/$defs/fraction"
        },
        "submit_rate": {
            "$ref": "#/$defs/union_bytes_rate"
        }
    },
    "additionalProperties": false,
    "$defs": {
        "bytes_size": {
            "type": "string",
            "pattern": "^\\d+(\\.\\d+)? ([KMGTPEZY]?i)?B$"
        },
        "union_type_bytes_size": {
            "oneOf": [
                {
                    "$ref": "#/$defs/bytes_size"
                },
                {
                    "type": "integer",
                    "minimum": 0
                }
            ]
        },
        "bytes_rate": {
            "type": "string",
            "pattern": "^\\d+(\\.\\d+)? ([KMGTPEZY]?i)?B\\/s$"
        },
        "union_type_bytes_rate": {
            "oneOf": [
                {
                    "$ref": "#/$defs/bytes_rate"
                },
                {
                    "type": "integer",
                    "minimum": 0
                }
            ]
        },
        "fraction": {
            "type": "number",
            "minimum": 0.0,
            "maximum": 1.0
        }
    }
}
"""

multi_field_schema = """
{
    "$schema": "http://json-schema.org/draft-07/schema",
    "$id": "https://simulator/workload/models/random_parameters_schema.json",
    "type": "object",
    "properties": {
        "data_set": {
            "$id": "#data_set",
            "type": "object",
            "properties": {
                "size": {
                    "$ref": "#/$defs/bytes_size"
                },
                "size_bytes": {
                    "type": "integer",
                    "minimum": 0
                },
                "file_size": {
                    "$ref": "#/$defs/bytes_size"
                },
                "file_size_bytes": {
                    "type": "integer",
                    "minimum": 0
                }
            },
            "additionalProperties": false,
            "allOf": [
                {
                    "oneOf": [
                        {
                            "required": [
                                "size"
                            ]
                        },
                        {
                            "required": [
                                "size_bytes"
                            ]
                        }
                    ]
                },
                {
                    "oneOf": [
                        {
                            "required": [
                                "file_size"
                            ]
                        },
                        {
                            "required": [
                                "file_size_bytes"
                            ]
                        }
                    ]
                }
            ]
        },
        "no_of_tasks": {
            "$id": "#no_of_tasks",
            "type": "integer",
            "minimum": 0
        },
        "read_fraction": {
            "$ref": "#/$defs/fraction"
        },
        "submit_rate": {
            "$ref": "#/$defs/bytes_rate"
        },
        "submit_rate_bytes": {
            "type": "integer",
            "minimum": 0
        }
    },
    "additionalProperties": false,
    "required": [
        "data_set",
        "no_of_tasks",
        "read_fraction",
    ],
    "oneOf": [
        {
            "required": [
                "submit_rate"
            ]
        },
        {
            "required": [
                "submit_rate_bytes"
            ]
        }
    ],
    "$defs": {
        "bytes_size": {
            "type": "string",
            "pattern": "^\\d+(\\.\\d+)? ([KMGTPEZY]?i)?B$"
        },
        "bytes_rate": {
            "type": "string",
            "pattern": "^\\d+(\\.\\d+)? ([KMGTPEZY]?i)?B\\/s$"
        },
        "fraction": {
            "type": "number",
            "minimum": 0.0,
            "maximum": 1.0
        }
    }
}
"""
