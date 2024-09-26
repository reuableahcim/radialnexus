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

# Loading Nodes from ../domain/trees/csv/data/Exceptional_Trees_On_Oahu.csv
query = '''
LOAD CSV WITH HEADERS FROM "file:///../domain/trees/csv/data/Exceptional_Trees_On_Oahu.csv" AS row
	WITH row
	WHERE row.id IS NOT NULL
	MERGE (tree:Tree {id:toString(row.id)})
		SET tree.name = row.common_name
		SET tree.height = row.height
		SET tree.diameter = row.diameter
		SET tree.circumference = row.circumference
		SET tree.description = row.information

'''
graph.run(query)

# Loading Nodes from ../domain/trees/csv/data/Exceptional_Trees_On_Oahu.csv
query = '''
LOAD CSV WITH HEADERS FROM "file:///../domain/trees/csv/data/Exceptional_Trees_On_Oahu.csv" AS row
	WITH row
	WHERE row.species_code IS NOT NULL
	MERGE (species:Species {id:toString(row.species_code)})
		SET species.name = row.scientific_name

'''
graph.run(query)

# Loading Nodes from ../domain/trees/csv/data/Exceptional_Trees_On_Oahu.csv
query = '''
LOAD CSV WITH HEADERS FROM "file:///../domain/trees/csv/data/Exceptional_Trees_On_Oahu.csv" AS row
	WITH row
	WHERE row.name IS NOT NULL
	MERGE (location:Location {id:toString(row.name)})
		SET location.name = row.location
		SET location.lat = row.latitude
		SET location.long = row.longitude

'''
graph.run(query)

# Loading Nodes from ../domain/trees/csv/data/Exceptional_Trees_On_Oahu.csv
query = '''
LOAD CSV WITH HEADERS FROM "file:///../domain/trees/csv/data/Exceptional_Trees_On_Oahu.csv" AS row
	WITH row
	WHERE row.island_abbv IS NOT NULL
	MERGE (island:Island {id:toString(row.island_abbv)})
		SET island.name = row.island_name

'''
graph.run(query)

# Loading Relationships from ../domain/trees/csv/data/Exceptional_Trees_On_Oahu.csv
query = '''
LOAD CSV WITH HEADERS FROM "file:///../domain/trees/csv/data/Exceptional_Trees_On_Oahu.csv" AS row
	MATCH (source:Tree {id: toString(row.id)}), (target:Location {id: toString(row.name)})
		MERGE(source)-[edge:IN_A]->(target)
'''
graph.run(query)

# Loading Relationships from ../domain/trees/csv/data/Exceptional_Trees_On_Oahu.csv
query = '''
LOAD CSV WITH HEADERS FROM "file:///../domain/trees/csv/data/Exceptional_Trees_On_Oahu.csv" AS row
	MATCH (source:Tree {id: toString(row.id)}), (target:Species {id: toString(row.species_code)})
		MERGE(source)-[edge:IS_A]->(target)
'''
graph.run(query)

# Loading Relationships from ../domain/trees/csv/data/Exceptional_Trees_On_Oahu.csv
query = '''
LOAD CSV WITH HEADERS FROM "file:///../domain/trees/csv/data/Exceptional_Trees_On_Oahu.csv" AS row
	MATCH (source:Location {id: toString(row.name)}), (target:Island {id: toString(row.island_abbv)})
		MERGE(source)-[edge:ON_A]->(target)
'''
graph.run(query)
