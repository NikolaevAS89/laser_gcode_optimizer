import re
from typing import Iterable
from typing import Iterator

s_macher = re.compile("(.*)(S[0-9.]+)(.*)")
x_macher = re.compile("(.*)(X[0-9.]+)(.*)")
y_macher = re.compile("(.*)(Y[0-9.]+)(.*)")
f_macher = re.compile("(.*)(F[0-9.]+)(.*)")


class Laser:
    def __init__(self):
        self._x = 0.0
        self._y = 0.0
        self._power = 0.0
        self._speed = 0.0
        self._is_moved = False

    def is_on(self) -> bool:
        return self._power > 0

    def command(self, command: str):
        self._is_moved = False
        if s_macher.match(command):
            self._power = float(s_macher.sub(r"\2", command)[1:].strip())
        if x_macher.match(command):
            x = round(float(x_macher.sub(r"\2", command)[1:].strip()), 1)
            self._is_moved = self._is_moved or x != self._x
            self._x = x
        if y_macher.match(command):
            y = round(float(y_macher.sub(r"\2", command)[1:].strip()), 1)
            self._is_moved = self._is_moved or y != self._y
            self._y = y
        if f_macher.match(command):
            self._speed = float(f_macher.sub(r"\2", command)[1:].strip())

    @property
    def x(self) -> float:
        return self._x

    @property
    def y(self) -> float:
        return self._y

    @property
    def power(self) -> float:
        return self._power

    @property
    def speed(self) -> float:
        return self._speed

    @property
    def is_moved(self) -> bool:
        return self._is_moved


class Point:
    _x: float
    _y: float

    def __init__(self, x: float, y: float):
        self._x = x
        self._y = y

    def __str__(self):
        return f'X{str(self._x)}Y{str(self._y)}'

    @property
    def x(self) -> float:
        return self._x

    @property
    def y(self) -> float:
        return self._y

    @staticmethod
    def parse_x(coord: str) -> float:
        return round(float(x_macher.sub(r"\2", coord)[1:].strip()), 1)

    @staticmethod
    def parse_y(coord: str) -> float:
        return round(float(y_macher.sub(r"\2", coord)[1:].strip()), 1)

    @staticmethod
    def compare(p1, p2) -> float:
        d = (p1.x + p1.y) - (p2.x + p2.y)
        return (p1.x - p2.x) if d == 0 else d

    @staticmethod
    def length(p1, p2):
        ax = (p1.x - p2.x)
        ay = (p1.y - p2.y)
        return (ax * ax + ay * ay) ** 0.5


class Edge:
    _point_a: Point
    _point_b: Point

    def __init__(self, point_a: Point, point_b: Point = None):
        self._point_a = point_a
        self._point_b = point_a if point_b is None else point_b

    def length(self):
        return Point.length(self._point_b, self._point_a)

    def extend(self, point: Point) -> bool:
        if not self._is_on_line(point):
            return False
        elif self._is_middle(point):
            return False
        else:
            self._point_b = point
            return True

    def to_plot_points(self):
        return [[self._point_a.x, self._point_b.x],
                [self._point_a.y, self._point_b.y]]

    def _is_middle(self, point: Point) -> bool:
        alpha = 1 if self._point_a.x < self._point_b.x else -1
        beta = 1 if self._point_a.y < self._point_b.y else -1
        check_x = (alpha * self._point_a.x <= alpha * point.x <= alpha * self._point_b.x)
        check_y = (beta * self._point_a.y <= beta * point.y <= beta * self._point_b.y)
        return check_x and check_y

    def _is_on_line(self, point: Point) -> bool:
        ax = (self._point_a.x - self._point_b.x)
        ay = (self._point_a.y - self._point_b.y)
        bx = (point.x - self._point_b.x)
        by = (point.y - self._point_b.y)
        return ax * by - ay * bx == 0

    @property
    def point_a(self) -> Point:
        return self._point_a

    @property
    def point_b(self) -> Point:
        return self._point_b


class GCodeFileReader(Iterable):

    def __init__(self, filename: str):
        self._filename_ = filename
        self._laser = Laser()

    def __iter__(self) -> Iterator[Edge]:
        with open(self._filename_, 'r') as gcode:
            command = gcode.readline().strip()
            line = None
            while command:
                self._laser.command(command)
                if self._laser.is_on():
                    x = self._laser.x
                    y = self._laser.y
                    point = Point(x, y)
                    if line is None:
                        line = Edge(point)
                    elif not line.extend(point):
                        break_line = line
                        line = Edge(line.point_b, point)
                        yield break_line
                elif line is not None:
                    yield line
                    line = None
                command = gcode.readline()
