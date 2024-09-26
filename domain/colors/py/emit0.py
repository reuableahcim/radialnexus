import argparse

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

class ColorGraphGenerator:
    SIZE = "33"
    ENDPOINT = "FFFFFF"

    def __init__(self):
        self.created_cells = set()

    def calculate_next_point(self, current_point, dimension, direction, size=SIZE):
        current_value = int(current_point, 16)
        change = int(size, 16)

        new_value = min(255, max(0, current_value + change if direction == "UP" else current_value - change))

        # Convert back to HEX format
        return f"{new_value:02X}"

    def emit(self, starting_point, direction, size=SIZE, endpoint=ENDPOINT):
        # Check if the cell has already been created
        if starting_point in self.created_cells:
            return

        # Create the cell dictionary
        cell = {"id": starting_point, "name": starting_point, "color": starting_point}
        self.created_cells.add(starting_point)

        # Use list comprehension to generate edge_list
        self.edge_list = [
            {"name": f"{dim}_{direction}", "source": starting_point, "target": self.calculate_next_point(starting_point, dim, direction)}
            for dim in ["RED", "GREEN", "BLUE"]
        ]

        # Recursive call for the next cell using list comprehension
        [self.emit(edge["target"], direction) for edge in self.edge_list]

        return cell

    def main(self, starting_point, direction):
        node_list = [self.emit(starting_point, direction)]
        print("Node List:", node_list)
        print("Edge List:", self.edge_list)

if __name__ == '__main__':

    Esse.printf("BEGIN...\n", RED)
    parser = argparse.ArgumentParser()
    parser.add_argument('starting_point', help="Starting Point")
    parser.add_argument('direction', help="Direction")
    args = parser.parse_args()

    generator = ColorGraphGenerator()
    generator.main(args.starting_point, args.direction)

    Esse.printf("\n...END", RED)    

# python3 emit.py 000000 UP
