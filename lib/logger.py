import logging
import sys
import os
if 'NUMEXPR_MAX_THREADS' not in os.environ:
    os.environ['NUMEXPR_MAX_THREADS']='12'

logger = logging.root

logging.basicConfig(
    # format="%(asctime)s:%(levelname)s:%(filename)s:%(lineno)s:%(message)s",
    format="%(filename)s:%(lineno)s:%(message)s",
    level=logging.INFO,
)

def lg(*args, hcolor="", color="", call=None, **kw):
    msg = ""
    if len(args):
        args = list(map(str, args))
        if hcolor:
            msg = colorMsg(args[0], hcolor)
            msg += ":"+",".join(args[1:])
        else:
            msg = ",".join(args)
    if kw:
        msg += str(kw)
    if call:
        msg = call(msg)
    filename = sys._getframe(1).f_code.co_filename[-15:]
    fileno = sys._getframe(1).f_lineno
    msg = f"{filename}:{fileno}:{msg}"
    if color:
        msg = colorMsg(msg, color)
    print(msg)
    return msg

def colorMsg(msg:str, color:str=""):
    if color == "red":
        msg = (bcolors.RED + msg + bcolors.ENDC)
    elif color == "warn":
        msg = (bcolors.WARNING + msg + bcolors.ENDC)
    elif color == "ok":
        msg = (bcolors.OKGREEN + msg + bcolors.ENDC)
    elif color == "blue":
        msg = (bcolors.OKBLUE + msg + bcolors.ENDC)
    return msg



class bcolors:
    ENDC = "\033[0m"
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    RED = "\033[41m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


# logger.lg = lg.__get__(logger)

# logger.debug= insertBatch.__get__(cursor)

