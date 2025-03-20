import json

from stream.io import GCodeFileReader
from stream.stream import Stream


def to_point(command: str) -> tuple[float, float]:
    parts = (command + "Y").split("Y")
    x = parts[0].replace("X", "")
    y = parts[1]
    px = float(x) if len(x) > 0 else 0
    py = float(y) if len(y) > 0 else 0
    return px, py


def compute_end_point(command: dict) -> dict:
    start = to_point(command.get("start", "X-1Y-1"))
    end_x = start[0]
    end_y = start[1]
    for point in command["points"]:
        p = to_point(point)
        end_x = p[0] if p[0] > 0 else p[0]
        end_y = p[1] if p[1] > 0 else p[1]
    command["a_end"] = start
    command["a_start"] = start
    return command


if __name__ == '__main__':
    gcode_reader = GCodeFileReader("0250.NOT_OPPTIMIZE.gcode")
    t = Stream(gcode_reader) \
        .map(lambda item: json.dumps(item)) \
        .for_each(lambda item: print(item))
