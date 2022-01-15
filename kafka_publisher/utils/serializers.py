import json
from decimal import Decimal

import datetime


class KafkaDefaultEncoder(json.JSONEncoder):
    """This class serializes JSON objects to strings with custom
    configurations for the following types:
    - Decimals -> floats
    - datetime -> ISO formatted strings

    All others are performed by the default `json.JSONEncoder.default`
    method
    """
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)
