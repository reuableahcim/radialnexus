import argparse
import itertools
import json

SIZE = "33"
ENDPOINT = "FFFFFF"

# Colors for printing
RED = "31;1"
GREEN = "32;1"
YELLOW = "33;1"
BLUE = "34;1"

EDGE_COLOR ={
    "RED_UP": "FF0000",
    "GREEN_UP": "00FF00",
    "BLUE_UP": "0000FF",
    "UP": "FFFFFF"
}

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
        seen = updated_point in self.created_cells
        
        Esse.printf("{updated_point}: {seen} [{direction}]", BLUE, updated_point=updated_point, seen=seen, direction=direction)
    
        # if updated_point not in self.created_cells:
        if not seen:
            self.created_cells.add(updated_point)

            node = {"id": updated_point, "name": updated_point, "color": updated_point, "size": 8192}

            edge_color = EDGE_COLOR[direction]
            edges = [
                {"name": f"{direction}", "source": current_point, "target": updated_point, "color": f"{edge_color}", "strength": 1024}
            ]

            # Use comprehensions for recursive calls
            child_nodes, child_edges = zip(*(
                self._cell(updated_point, dim_direction, size, endpoint)
                for dim_direction in ["RED_UP", "GREEN_UP", "BLUE_UP"]
                if dim_direction != direction and not seen
            ))

            # return [node, *child_nodes], [*edges, *child_edges]
            return [node, *list(itertools.chain(*child_nodes))], list(itertools.chain(edges, *child_edges))            
        else:
            return [], []

def main(starting_point, direction, dict_file_path):
    graph_generator = GraphGenerator()
    node_list, edge_list = graph_generator.emit(starting_point, direction)
    print("Node List:", node_list)
    print("Edge List:", edge_list)
    map_dict = {"nodes": node_list, "links": edge_list}     
    with open(dict_file_path, 'w') as  dict_file:
        Esse.printf("Generating...", RED)
        json.dump(map_dict, dict_file, indent=4)
        Esse.printf("...done", RED)


if __name__ == '__main__':

    Esse.printf("BEGIN...\n", RED)
    parser = argparse.ArgumentParser()
    parser.add_argument('starting_point', help="Starting Point")
    parser.add_argument('direction', help="Direction")
    parser.add_argument('target_filename', help="Target File")
    args = parser.parse_args()
    main(args.starting_point, args.direction, args.target_filename)

    Esse.printf("\n...END", RED)    

# python3 emit.py 000000 UP ../json/colors.json