import math
import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).parent.absolute().resolve() / 'src'))

from geographiclib.geodesic import Geodesic
from medsutil.math.helpers import numerical_derivative

wgs = Geodesic.WGS84

def distance(y1, x1, y2, x2, /):
    return wgs.Inverse(y1, x1, y2, x2)["s12"]

d_y1 = numerical_derivative(distance, 0, float)
d_y2 = numerical_derivative(distance, 2, float)
d_x1 = numerical_derivative(distance, 1, float)
d_x2 = numerical_derivative(distance, 3, float)

def distance_and_derivatives(y1, x1, y2, x2, dy1, dx1, dy2, dx2, /):
    print(distance(y1, x1, y2, x2))
    print(d_y1(y1, x1, y2, x2) * dy1)
    print(d_y2(y1, x1, y2, x2) * dy2)
    print(d_x1(y1, x1, y2, x2) * dx1)
    print(d_x2(y1, x1, y2, x2) * dx2)

cp = 0.00001
distance_and_derivatives(0, 0, 5, 5, cp, cp, cp, cp)
