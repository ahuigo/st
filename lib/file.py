import json
import datetime


class DateEnconding(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime.date):
            return o.strftime("%Y/%m/%d")


def save(data, jsonPath):
    with open(jsonPath, "w") as f:
        json.dump(data, f, cls=DateEnconding)
