import typing as t

import datetime
import json

from decimal import Decimal


class KafkaDefaultEncoder(json.JSONEncoder):
    """This class serializes JSON objects to strings with custom
    configurations for the following types:
    - Decimals -> floats
    - datetime -> ISO formatted strings

    All others are performed by the default `json.JSONEncoder.default`
    method
    """

    def default(self, obj: t.Any) -> t.Any:
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)
