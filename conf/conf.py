import os,sys
from pathlib import Path
print('Running' if __name__ == '__main__' else 'Importing', Path(__file__).resolve())


#if __name__ == "__main__":
if '.' not in sys.path:
    sys.path.append(".")


# pg数据库
dbconf = {"database": "ahuigo", "user": "role1", "password": "", "host": "127.0.0.1"}
# debug
# Dtrade=1 BIG=1   python -u tool/bench.py  -c 6 -p 40 --dstep=1230:1231
DEBUG = os.getenv("DEBUG", "")
Dtrade = os.getenv("Dtrade", "")
CODE = os.getenv("CODE", "")
REFRESH = os.getenv("REFRESH", "")
PUBLISH = os.getenv("PUBLISH", "")
BIG = os.getenv("BIG", False)
big = False
min_step = 2
max_step = 1

