import json
import logging
import sys
import pathlib
import traceback
from pprint import pprint

sys.path.append(str(pathlib.Path(__file__).parent.absolute().resolve() / 'src'))


from medsutil.ocproc2 import SingleElement

se = SingleElement([1,2,3])
print(se.is_list_like())
print(se.is_string_like())

se = SingleElement(5)
print(se.is_list_like())
print(se.is_string_like())


se = SingleElement("foobar")
print(se.is_list_like())
print(se.is_string_like())