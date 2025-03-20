import re
from typing import Iterable
from typing import Iterator

s_macher = re.compile("(.*)(S[0-9.]+)(.*)")
x_macher = re.compile("(.*)(X[0-9.]+)(.*)")
y_macher = re.compile("(.*)(Y[0-9.]+)(.*)")
f_macher = re.compile("(.*)(F[0-9.]+)(.*)")


class Laser:
    def __init__(self):
        self.prev_x = 0.0
        self.prev_y = 0.0
        self._x = 0.0
        self._y = 0.0
        self._power = 0.0
        self._speed = 0.0

    def command(self, command: str):
        if s_macher.match(command):
            self._power = float(s_macher.sub(r"\2", command)[1:].strip())
        if x_macher.match(command):
            self._x = float(x_macher.sub(r"\2", command)[1:].strip())
        if y_macher.match(command):
            self._y = float(y_macher.sub(r"\2", command)[1:].strip())
        if f_macher.match(command):
            self._speed = float(f_macher.sub(r"\2", command)[1:].strip())

    @property
    def x(self):
        return self._x

    @property
    def y(self):
        return self._y

    @property
    def power(self):
        return self._power

    @property
    def speed(self):
        return self._speed


class GCodeFileReader(Iterable):

    def __init__(self, filename: str):
        self._filename_ = filename
        self._laser = Laser()

    def __iter__(self) -> Iterator[str]:
        with open(self._filename_, 'r') as gcode:
            line = gcode.readline().strip()
            while line:
                x = self._laser.x
                y = self._laser.y
                p = self._laser.power
                s = self._laser.speed
                self._laser.command(line)
                if p != self._laser.power or s != self._laser.speed:
                    yield {"x": x, "y": y, "p": p, "s": s}
                    yield {"x": self._laser.x, "y": self._laser.y, "p": self._laser.power, "s": self._laser.speed}
                elif x != self._laser.x and y == self._laser.y:
                    pass
                elif x == self._laser.x and y != self._laser.y:
                    pass
                elif x == self._laser.x and y == self._laser.y:
                    pass
                else:
                    yield {"x": x, "y": y, "p": p, "s": s}
                    yield {"x": self._laser.x, "y": self._laser.y, "p": self._laser.power, "s": self._laser.speed}
                line = gcode.readline()
