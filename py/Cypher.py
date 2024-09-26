import os
import glob
import argparse
import chardet
import fnmatch
import pprint
import re
import pandas as pd
import numpy as np
import math
import json
from py2neo import Graph, Node, Relationship

# Constants
RED = "31;1"
GREEN = "32;1"
YELLOW = "33;1"
BLUE = "34;1"

# Meta class for all classes
class Esse:
    # Print Formatted String with Keywords
    @staticmethod
    def printf(formatted_text, code, **kwargs):
        print(f"\033[{code}m{formatted_text.format(**kwargs)}\033[0m")

def delete_all_nodes(graph):
    graph.run("MATCH (n) DETACH DELETE n")
    
def delete_all_edges(graph):
    graph.run("MATCH ()-[r]-() DELETE r")    

def main(graph):
    
    delete_all_nodes(graph)
    delete_all_edges(graph)

if __name__ == '__main__':

    Esse.printf("BEGIN...\n", RED)
    neo4j_url = os.environ["NEO4J_BASE_URL"]
    neo4j_user = os.environ["NEO4J_USERNAME"]
    neo4j_pass = os.environ["NEO4J_PASSWORD"]
    
    Esse.printf("URL: {neo4j_url}", BLUE, neo4j_url=neo4j_url)

    graph = Graph(neo4j_url, auth=(neo4j_user, neo4j_pass))
    
    main(graph)
