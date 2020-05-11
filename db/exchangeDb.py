from db import keyvDb


@keyvDb.withCache("getTradeDate", 66400 * 1)
def getTradeDate(code):
    # todo: fetch trade Date
    # from datetime import date
    return "20191203"

