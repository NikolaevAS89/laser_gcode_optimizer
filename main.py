import matplotlib.pyplot as plt

from parser.io import GCodeFileReader, Edge, Point
from parser.stream import Stream


def add_edge(nodes: list[Point], matrix: dict[str, dict[str, float]], edge: Edge):
    key_a = str(edge.point_a)
    key_b = str(edge.point_b)
    nodes.append(edge.point_a)
    nodes.append(edge.point_b)
    sub_matrix = matrix.get(key_a, dict())
    sub_matrix[key_b] = edge.length()
    matrix[key_a] = sub_matrix
    sub_matrix = matrix.get(key_b, dict())
    sub_matrix[key_a] = edge.length()
    matrix[key_b] = sub_matrix


def do_step(matrix: dict[str, dict[str, float]],
            current: str) -> (str, float):
    step = None
    density = None
    for key, value in matrix.get(current).items():
        if value > 0:
            if density is None or density > value:
                density = value
                step = key
    if step is not None:
        matrix.get(step)[current] = -1
        matrix.get(current)[step] = -1
    return step, density


class Path:
    points: list[Point]
    desity: float
    is_cycled: bool

    def __init__(self, points: list[str], density: float):
        pass

    def distance(self, point: Point) -> float:
        if self.is_cycled:
            pass
        else:
            d_1 = Point.length(point, self.points[0])
            d_1 = Point.length(point, self.points[0])



def calculate_paths(nodes: list[Point], matrix: dict[str, dict[str, float]]) -> (list[list[str]], list[float]):
    paths = list()
    densities = list()
    path = list()
    path_length = 0.0
    sorted_nodes = sorted(nodes, key=lambda itm: itm.x - itm.y)
    for node in sorted_nodes:
        curr_step = str(node)
        path.append(curr_step)
        is_reversed = False
        while curr_step is not None:
            step_key, density = do_step(matrix, curr_step)
            if step_key is None:
                if not is_reversed:
                    step_key = path[0]
                    path.reverse()
                    is_reversed = True
                else:
                    if len(path) > 1:
                        paths.append(path)
                        densities.append(path_length)
                    path = list()
                    path_length = 0.0
            else:
                path.append(step_key)
                path_length += density
            curr_step = step_key
    return paths, densities


if __name__ == '__main__':
    gcode_reader = GCodeFileReader("0250.NOT_OPPTIMIZE.gcode")
    matrix = dict()
    nodes = list()
    plt.close('all')
    Stream(gcode_reader) \
        .filter(lambda item: item.length() > 0) \
        .peek(lambda item: add_edge(nodes, matrix, item)) \
        .for_each(lambda item: plt.plot(item.to_plot_points()[0], item.to_plot_points()[1], marker='o'))

    paths, densities = calculate_paths(nodes, matrix)

    idx = 0
    for path in paths:
        print(f'{str(densities[idx])}: {path}')
        idx += 1
        xs = [Point.parse_x(itm) for itm in path]
        ys = [Point.parse_y(itm) for itm in path]
        plt.plot(xs, ys, marker='o')
    # plt.plot(item.to_points()[0], item.to_points()[1], marker='o')

    # plt.xlabel('X-axis')
    # plt.ylabel('Y-axis')
    # plt.title('Line by Coordinates')
    plt.grid(True)
    # plt.legend()

    # Show the plot
    plt.show()
