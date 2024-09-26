import argparse

SIZE = "33"
ENDPOINT = "FFFFFF"

# Colors for printing
RED = "31;1"
GREEN = "32;1"
YELLOW = "33;1"
BLUE = "34;1"

class Esse:  
    # Print Formatted String with Keywords
    @staticmethod
    def printf(formatted_text, color, **kwargs):
        # TODO: Conditional?
        print(f"\033[{color}m{formatted_text.format(**kwargs)}\033[0m")

class GraphGenerator:
    def __init__(self):
        self.created_cells = set()

    def emit(self, starting_point, direction, size=SIZE, endpoint=ENDPOINT):
        """
        Generate nodes and edges for a 3D graph.

        Parameters:
        - starting_point (str): The starting point in RGB hex format.
        - direction (str): The direction of graph generation.
        - size (str): The unit of change in hex format.
        - endpoint (str): The endpoint in RGB hex format.

        Returns:
        - (list): A list of dictionaries representing nodes.
        - (list): A list of dictionaries representing edges.
        """
        return self._cell(starting_point, direction, size, endpoint)

    def _hex_to_int(self, hex_value):
        """Convert a hex value to an integer."""
        return int(hex_value, 16)

    def _int_to_hex(self, int_value):
        """Convert an integer value to hex."""
        return format(int_value, '02X')

    def _update_values(self, current_values, direction, size):
        """
        Update RGB values based on the given direction.

        Parameters:
        - current_values (list): List of current RGB values.
        - direction (str): The direction of graph generation.
        - size (str): The unit of change in hex format.

        Returns:
        - str: Updated RGB values as a hex string.
        """
        direction_mapping = {"RED_UP": 0, "GREEN_UP": 1, "BLUE_UP": 2}
        dimension_index = direction_mapping.get(direction, -1)
        if dimension_index != -1:
            current_values[dimension_index] = min(255, current_values[dimension_index] + self._hex_to_int(size))

        return ''.join(self._int_to_hex(value) for value in current_values)

    def _cell(self, current_point, direction, size, endpoint):
        """
        Recursively generate cells for a 3D graph.

        Parameters:
        - current_point (str): The current point in RGB hex format.
        - direction (str): The direction of graph generation.
        - size (str): The unit of change in hex format.
        - endpoint (str): The endpoint in RGB hex format.

        Returns:
        - (list): A list of dictionaries representing nodes.
        - (list): A list of dictionaries representing edges.
        """
        current_values = [self._hex_to_int(current_point[i:i+2]) for i in (0, 2, 4)]

        updated_point = self._update_values(current_values, direction, size)

        if updated_point not in self.created_cells:
            self.created_cells.add(updated_point)

            node = {"id": updated_point, "name": updated_point, "color": updated_point}

            edges = [
                {"name": f"{direction}_UP", "source": current_point, "target": updated_point}
            ]

            # Use comprehensions for recursive calls
            child_nodes, child_edges = zip(*(
                self._cell(updated_point, dim_direction, size, endpoint)
                for dim_direction in ["RED_UP", "GREEN_UP", "BLUE_UP"]
                if dim_direction != direction
            ))

            return [node, *child_nodes], [edge for edge_list in [edges, *child_edges] for edge in edge_list]
        else:
            return [], []

def main(starting_point, direction):
    graph_generator = GraphGenerator()
    node_list, edge_list = graph_generator.emit(starting_point, direction)
    print("Node List:", node_list)
    print("Edge List:", edge_list)


if __name__ == '__main__':

    Esse.printf("BEGIN...\n", RED)
    parser = argparse.ArgumentParser()
    parser.add_argument('starting_point', help="Starting Point")
    parser.add_argument('direction', help="Direction")
    args = parser.parse_args()
    main(args.starting_point, args.direction) 

    Esse.printf("\n...END", RED)    

# python3 emit.py 000000 UP
    
    