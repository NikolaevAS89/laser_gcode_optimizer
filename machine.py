import re

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
