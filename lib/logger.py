import logging
import sys
from typing import Union
import os
if 'NUMEXPR_MAX_THREADS' not in os.environ:
    os.environ['NUMEXPR_MAX_THREADS']='12'

logger = logging.root

logging.basicConfig(
    # format="%(asctime)s:%(levelname)s:%(filename)s:%(lineno)s:%(message)s",
    format="%(filename)s:%(lineno)s:%(message)s",
    level=logging.INFO,
)

# max filename
max_filename = 100
def setMaxFilename(l=100):
    global max_filename
    max_filename = l


'''''''''''
level
'''''''''''
FATAL = logging.CRITICAL
ERROR = logging.ERROR
WARN = logging.WARN
INFO = logging.INFO
DEBUG = logging.DEBUG
NOTSET = logging.NOTSET

slevel = logging.DEBUG
def setLevel(level=logging.DEBUG):
    global slevel
    slevel = level

def debug(*args, hcolor="", color="", call=None, **kw)->Union[str,None]:
    log(*args, hcolor=hcolor, color=color, call=call, level=logging.DEBUG, **kw)

def warn(*args, hcolor="", color="", call=None, **kw)->Union[str,None]:
    color = color or ColorName.WARN
    log(*args, hcolor=hcolor, color=color, call=call, level=logging.INFO, **kw)

def info(*args, hcolor="", color="", call=None, **kw)->Union[str,None]:
    log(*args, hcolor=hcolor, color=color, call=call, level=logging.INFO, **kw)

def error(*args, hcolor="", color="", call=None, **kw)->Union[str,None]:
    color = color or ColorName.RED
    log(*args, hcolor=hcolor, color=color, call=call, level=logging.ERROR, **kw)

def fatal(*args, hcolor="", color="", call=None, **kw)->Union[str,None]:
    color = color or ColorName.RED
    log(*args, hcolor=hcolor, color=color, call=call, level=logging.FATAL, **kw)

def log(*args, hcolor="", color="", call=None, level=logging.DEBUG, **kw)->Union[str,None]:
    if level<slevel:
        return 
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
    filename = sys._getframe(1).f_code.co_filename[-max_filename:]
    fileno = sys._getframe(1).f_lineno
    msg = f"{filename}:{fileno}:{msg}"
    if color:
        msg = colorMsg(msg, color)
    print(msg)
    return msg

def colorMsg(msg:str, color:str="")->str:
    if color == "red":
        msg = (bcolors.RED + msg + bcolors.ENDC)
    elif color == "warn":
        msg = (bcolors.WARNING + msg + bcolors.ENDC)
    elif color == "ok":
        msg = (bcolors.OKGREEN + msg + bcolors.ENDC)
    elif color == "blue":
        msg = (bcolors.OKBLUE + msg + bcolors.ENDC)
    return msg


class ColorName:
    RED = 'red'
    BLUE = 'blue'
    OK = 'ok'
    WARN = 'warn'

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

# logger.log = lg.__get__(logger)

# logger.debug= insertBatch.__get__(cursor)

