"""
Manifests the Graph from various inputs to various outputs using the Graph itself.

python3 manifest.py trees Graph Cypher Exceptional_Trees_On_Oahu

Parameters: Here's an explanation of the parameters above

    trees:      the top-level directory containing the Domain of Discourse
    Graph:      the desired Manifestation
    Cypher:     the required Transformation
    Source:     the current Source

Structure:  Here's the directory structure the example currently assumes

    domains/
        trees/
            02_Mapping.csv
            csv/
                data/
                info/
            json/
            py/

Process:  Here's an overview of the current implementation.

The overall goal of the script is to take a mapping of a graph in simple format (currently a 
spreadsheet, soon a graph itself) and manifest that mapping into a Graph.  It's envisaged that
the script will be used to mapping many different domains.  A number of assumptions have been
made about the directory structure as a result.  Under domains, will be the current Domain of
Interest.  There will be a subdirectory within that will correspond to the first parameter.  
From there, the script assumes the Mapping will be at the top of that subdirectory and will be
of the form *Mapping.csv (use a versioning system that starts with 2 digits and an underscore).
From there, the script assumes the actual data files will be in csv and are in a subdirectory
so named within a data subdirectory therein.  

The key differentiating point for the script is its integration of the Graph into the Code and
its view of Code as a Graph.  Inheritance is used, with Classes corresponding to parameters so 
that the specific Transformer can be specified and executed against.

Execution:

First, install a Desktop version of Neo4j.  Then add to your shell profile:

    export NEO4J_USERNAME=neo4j
    export NEO4J_PASSWORD=<pwd>
    export NEO4J_BOLT_URL="bolt://$NEO4J_USERNAME:$NEO4J_PASSWORD@localhost:7687"
    export NEO4J_BASE_URL="bolt:/localhost:7687"

From a subdirectory parallel to domains, run the method as follows:

    python3 manifest.py trees Graph Cypher Exceptional_Trees_On_Oahu

Then in the py subdirectory there will be 

    <domain>.py 

script generated.  Run that script:

    python3 <domain>.py

to load the graph.  Share and Enjoy.

Copyright(c), 2023, Michael Bauer.  All Rights Reserved.


"""
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
import yaml

#import unicodedata
from unidecode import unidecode

# Constants
RED = "31;1"
GREEN = "32;1"
YELLOW = "33;1"
BLUE = "34;1"

CWD = os.getcwd()
ROOT_PATH = "../"
DOMAIN_PITH = "domain"
DOMAIN_PATH = ROOT_PATH + DOMAIN_PITH
CONFIG_PITH = "yaml"
MANIFEST_PITH = "manifest"

MODEL_PITH = "Model"
MAPPING_PITH = "Mapping"

#CODE_PITH = "py"
#INPUT_PITH = "csv"
#OUTPUT_PITH = "json"
#DATA_PITH = "data"

FRAMEWORK_PITH = "framework"
TEMPLATE_PITH = "template"
FRAMEWORK_FILE_NAME = FRAMEWORK_PITH + "." + CODE_PITH
TEMPLATE_FILE_NAME = TEMPLATE_PITH + "." + CODE_PITH
ID_PROPERTY = "id"
SOURCE_COLUMN = "Column"
GENERATION = "generate"
SAMPLE_INPUT_SIZE = 10000
DEFAULT_ENCODING = 'UTF-8'

# Meta class for all classes
class Esse:
    # Instantiate any Class dynamically
    @staticmethod
    def instantiate(class_type, *args, **kwargs):
        # Esse.printf("____________Esse: Instantiate____________\n", GREEN)             
        if class_type not in globals():
            raise ValueError(f"Invalid Class Type: {class_type}")
        instance = globals()[class_type](*args, **kwargs)
        return instance

    # Extricate method from instance by name     
    @staticmethod
    def extricate(instance, method_name):
        #Esse.printf("____________Esse: Extricate____________\n", GREEN)             
        #Esse.printf("Method: {method_name}", YELLOW, method_name=method_name)
        method = getattr(instance, method_name, None)
        return method

    # Get a class by name
    def get_class(self, class_name):
        if class_name not in globals():
            raise ValueError(f"Invalid Class Name: {class_name}")
        return globals()[class_name]

    # Get a constant from a class 
    def get_constant(self, class_ref, constant_name):
        if not hasattr(class_ref, constant_name):
            raise AttributeError(f"Constant '{constant_name}' not found in class '{class_ref.__name__}'")    
        return getattr(class_ref, constant_name)
        
    # General "Print" statement. 
    def inspect(self):
        esse = type(self).__name__
        self.printf("{esse}:", RED, esse=esse)
        attributes = {attr: getattr(self, attr) for attr in dir(self) if not attr.startswith("__") and not callable(getattr(self, attr))}
        [print(f"{key}:", value) for key, value in attributes.items()]
        print("\n")

    # Get Variable name from a Value
    def get_variable_name(self, name, qualifier=None):
        name = self.knas(name)
        variable = name.lower()
        return variable
    
    # Get Method name from a Value
    def get_method_name(self, name, qualifier=None):
        name = self.knas(name)
        variable = name.lower()
        return variable

    # Know Null As String
    def knas(self, value):
        knas = value
        if isinstance(value, (int,float)):
            if math.isnan(value):
                knas = ""
            else:
                knas = value
        if knas is None:
            knas = ""
        return knas
    
    # Print any Type dynamically
    @staticmethod
    def print_type(esse):
        esse_type = type(esse)
        esse_name = str(esse_type)[8:-2]
        Esse.printf("Esse: {esse_name}", RED, esse_name=esse_name)
        
    # Print Formatted String with Keywords
    @staticmethod
    def printf(formatted_text, code, **kwargs):
        # TODO: Conditional?
        print(f"\033[{code}m{formatted_text.format(**kwargs)}\033[0m")

    @staticmethod
    def uniexcode(input_str):
        # Using unicodedata
        # nfkd_form = unicodedata.normalize('NFKD', input_str)
        # normalized_form = ''.join([c for c in nfkd_form if not unicodedata.combining(c)])
        # Using unidecode
        normalized_form = input_str
        normalized_form = unidecode(input_str)
        normalized_form = normalized_form.replace('`', '')
        #normalized_form = normalized_form.replace("'", "")        
        return normalized_form

# Meta Python convenience class
class Struct(Esse):
    def __init__(self):
        pass
    
# Dictionary convenience class
class Dict(Struct):
    # Print Dictionary using json
    @staticmethod
    def printf(dictionary):
        Esse.printf("Dictionary:", RED)
        # print(json.dumps(dictionary, indent=4))
        formatted_output = '\n\n'.join(f"{key}: {value}" for key, value in dictionary.items())
        print(formatted_output)        

# Data Frame convenience class
class DF(Struct):
    # Split Data Frame into a Dictionary of Data Frames based on a column, retaining sort order
    @staticmethod
    def split(df, column):
        df_dict = {}
        if df is not None:
            grouped = df.groupby(column, sort=False)
            df_dict = { column: group for column, group in grouped}
        return df_dict
    
    # Extract Series from Data Frame by Column Value
    @staticmethod
    def extract_series(node_name, node_df, column, value):
        Esse.printf("____________extract_row____________\n", YELLOW)
        DF.printf(node_name, node_df)
        
        series = node_df[node_df[column] == value].iloc[0]
        redux_df = node_df[node_df[column] != value]
        return series, redux_df

    # Print a Data Frame
    @staticmethod
    def printf(name, df, code=RED):
        Esse.printf("{name} Data Frame:", code, name=name)        
        print(df)
        print("\n")

    # Print a Dictionary of Data Frames
    @staticmethod
    def print_dfs(dataframes):
        formatted_output = '\n\n'.join(f"{key}: {value}" for key, value in dataframes.items())
        print(formatted_text)

# Series convenience class
class Series(Struct):
   # Print a Data Frame
   @staticmethod
   def printf(name, series, code=RED):
       Esse.printf("{name} Series:", code, name=name)        
       print(series)
       print("\n")
    
# File interrogation and extraction
class File(Esse):
    # Initialize the File Attributes (path, base, pith)
    def __init__(self, path):
        self.path = path
        try:
            self.base = os.path.basename(path)
        except TypeError as ve:
            Esse.printf(f"Got File? TypeError: {ve}\n", RED)
            raise
        self.pith = File.get_pith(self.base)
    
    # Set the encoding for the file (particularly for CSV files)  
    def set_encoding(self):
        with open(self.path, 'rb') as f:
            result = chardet.detect(f.read(SAMPLE_INPUT_SIZE))
            encoding = result['encoding']
        self.encoding = encoding
        
    # Get the pith (aka filename from filename.ext)
    @staticmethod
    def get_pith(basename):
        pith = os.path.splitext(basename)[0]
        return pith
    
    # Find the file with substring (default to csv)
    @staticmethod
    def find_matching_file(directory_path, substring, extension=INPUT_PITH):
        Esse.printf("Find: {substring} Exension: {extension}", YELLOW, substring=substring, extension=extension)    
        files = glob.glob(os.path.join(directory_path, f"*{substring}.{extension}"))
        return files[0] if files else None

# Scripts generated for loading
class Script(File):
    def generate(self, template_file_path, contents):
        Esse.printf("____________Script: Generate____________\n", GREEN)  
        script_file_path = self.path          
        with open(script_file_path, 'w') as script_file:
            with open(template_file_path, 'r') as template_file:
                Esse.printf("Generating...", RED)
                script_file.write(template_file.read())
                script_file.write(contents)
                Esse.printf("...done", RED)        

# List Files
class ListFile(File):
    def __init__(self, list_file_directory, list_file_name):
        list_file_path = list_file_directory  + "/" + list_file_name + "." + OUTPUT_PITH
        self.pith = list_file_name
        self.path = list_file_path

    def generate(self, node_code, edge_code):
        pith = self.pith
        list_file_path = self.path
        Esse.printf("____________ListFile: Generate {pith}: {list_file_path}____________\n", GREEN, pith=pith, list_file_path=list_file_path)  
        content = node_code + edge_code
        with open(list_file_path, 'w') as  list_file:
            Esse.printf("Generating...", RED)
            list_file.write(content)
            Esse.printf("...done", RED) 

# Dictionary Files    
class DictFile(File):
    def __init__(self, dict_file_directory, dict_file_name):
        dict_file_path = dict_file_directory  + "/" + dict_file_name + "." + OUTPUT_PITH
        self.pith = dict_file_name
        self.path = dict_file_path

    def generate(self, map_dict):
        pith = self.pith
        dict_file_path = self.path
        Esse.printf("____________DictFile: Generate {pith}: {dict_file_path}____________\n", GREEN, pith=pith, dict_file_path=dict_file_path)  
        with open(dict_file_path, 'w') as  dict_file:
            Esse.printf("Generating...", RED)
            json.dump(map_dict, dict_file, indent=4)
            Esse.printf("...done", RED) 
        
# Transformer for Domain (Cypher or NeoModel)
class Transformer(Esse):
    COLUMN = 'Property'

    def __init__(self, manifestation):
        Esse.printf("____________Transformer____________\n", GREEN)                
        # Pattern is that Child will set itself and Parent        
        manifestation.transformer = self
        self.parent = manifestation
        self.inspect()
         
# Cypher Transformer Class
class Cypher(Transformer):

    # TODO: Manifest
    def __init__(self, domain):
        Esse.printf("____________Cypher____________\n", GREEN)                
        super().__init__(domain)
        self.column = CYPHER_COLUMN
        
    def get_transform(self, series, column_column):
        column = series[column_column]
        code = f"row.{column}"
        return code
        
    def node_assignments(self, variable_name, node_df):
        Esse.printf("____________Cypher: node_assignment {variable_name}____________\n", GREEN, variable_name=variable_name)
        code = ""
        target_column = self.column
        if not node_df.empty:
            code += "".join([f"\t\tSET {variable_name}.{row[target_column]} = row.{row[SOURCE_COLUMN]}\n" for index, row in node_df.iterrows()])
        return code
    
    def node_set(self, node_name, node_df):
        Esse.printf("____________Cypher: node_set {node_name}____________\n", GREEN, node_name=node_name)
        DF.printf(node_name, node_df)
        variable_name = self.get_variable_name(node_name)
        code = ""
        if node_df is not None:
            code += self.node_assignments(variable_name, node_df)
        if code.endswith(","):
            code = code.rstrip(",")
        code += "\n"
        return code
    
    def node_merge(self, node_name, id_series):
        Esse.printf("____________Cypher: node_merge {node_name}____________\n", GREEN, node_name=node_name)
        variable_name = self.get_variable_name(node_name)
        code = ""
        if not id_series.empty:
            identifier_transform = self.get_transform(id_series, SOURCE_COLUMN)
            code += f"\tWITH row\n"
            code += f"\tWHERE {identifier_transform} IS NOT NULL\n"       
            code += f"\tMERGE ({variable_name}:{node_name} "
            code += f"{{id: {identifier_transform}}}"   
            code += ")\n"  
        return code
    
    def node_body(self, node_name, node_df):
        Esse.printf("____________Cypher: node_body {node_name}____________\n", GREEN, node_name=node_name)
        code = ""
        target_column = self.column
        id_series, redux_df = DF.extract_series(node_name, node_df, target_column, ID_PROPERTY)
        code += self.node_merge(node_name, id_series)
        code += self.node_set(node_name, redux_df)
        return code

    def node_header(self, file_name):
        code = ""
        code += f"\n# Loading Nodes from {file_name}"
        code += f"\nquery = '''\n"
        code += f"LOAD CSV WITH HEADERS FROM \"file:///{file_name}\" AS row\n"
        return code

    def node_footer(self):
        code = ""
        code += f"'''"
        code += f"\ngraph.run(query)"
        code += f"\n"
        return code

    def node(self, file_name, node_name, node_df):
        Esse.printf("____________Cypher: node {node_name}____________\n", GREEN, node_name=node_name)
        code = ""
        code += self.node_header(file_name)
        code += self.node_body(node_name, node_df)
        code += self.node_footer()
        return code
    
    def nodes(self, file_name, mappings):
        Esse.printf("____________Cypher: nodes {file_name}____________\n", RED, file_name=file_name)
        code = ""
        code += "".join([self.node(file_name, node_name, node_df) for node_name, node_df in mappings.items()])
        return code

    def edge_line(self, series):
        Esse.printf("____________Cypher: edge_line____________\n", GREEN)
        code = ""
        
        source_variable = "source"
        source_node = series['Source Node']
        source_identity = series['Source ID']
        target_variable = "target"
        target_node = series['Target Node']
        target_identity = series['Target ID']
        relationship = series['Relationship']

        code += "\tMATCH "
        code += f"({source_variable}:{source_node} {{id: row.{source_identity}}}), ({target_variable}:{target_node} {{id: row.{target_identity}}})\n"
        code += "\t\tMERGE"
        code += f"({source_variable})-[edge:{relationship}]->({target_variable})\n"

        return code

    def edge_body(self, edge_name, edge_df):
        Esse.printf("____________Cypher: edge_body {edge_name}____________\n", GREEN, edge_name=edge_name)
        code = ""  
        code += "".join([self.edge_line(series) for _, series in edge_df.iterrows()])
        return code

    def edge_header(self, file_name):
        code = ""
        code += f"\n# Loading Relationships from {file_name}"
        code += f"\nquery = '''\n"
        code += f"LOAD CSV WITH HEADERS FROM \"file:///{file_name}\" AS row\n"
        return code

    def edge_footer(self):
        code = ""
        code += f"'''"
        code += f"\ngraph.run(query)"
        code += f"\n"
        return code

    def edge(self, file_name, edge_name, edge_df):
        Esse.printf("____________Cypher: edge {file_name}____________\n", GREEN, file_name=file_name)
        code = ""
        code += self.edge_header(file_name)
        code += self.edge_body(edge_name, edge_df)
        code += self.edge_footer()
        return code
    
    def edges(self, file_name, mappings):
        Esse.printf("____________Cypher: edges____________\n", GREEN, file_name=file_name)
        Dict.printf(mappings)
        code = ""
        code += "".join([self.edge(file_name, edge_name, edge_df) for edge_name, edge_df in mappings.items()])
        return code

    def generate(self, node_code, edge_code, output_directory_path=""):
        code = node_code + edge_code
        directory = self.directory
        
        domain_file_path = directory.domain_file_path
        template_file_path = directory.template_file_path
        
        script = Script(domain_file_path)
        script.generate(template_file_path, code)
                
# NeoModel Transformer Class
class NeoModel(Transformer):
    def __init__(self, domain):
        Esse.printf("____________NeoModel____________\n", GREEN)                
        super().__init__(domain)

# 3D-Force Graph Transformer Class
class Force(Transformer):
    COLUMN = 'Attribute'

    # TODO: set output types
    def __init__(self, manifestation):
        super().__init__(manifestation)
        Esse.printf("____________Force____________\n", GREEN)
        data_file_path = self.parent.directory.data_file_path
        data_file = DataFile(data_file_path)
        pith = data_file.pith
        df = data_file.ingest()
        DF.printf(pith, df)
        self.data = df
        # TODO: set output directory
        self.inspect()       

    def id(self, value):
        value = value.replace(',','')
        return value
        
    def node(self, index, row, mappings):
        # Esse.printf("____________Force: map____________\n", GREEN)
        # id: A unique identifier for the node (required).
        # name: A display name for the node.
        # color: To specify the node color.
        # val: Another way to define the node's volume (often affects visual size).
        # desc: text
        # size: To specify the size of the node
        node_dicts = []
        for node_name, node_df in mappings.items():
            node_dict = {}
            color = '95CDEA'
            for _, mapping_row in node_df.iterrows():
                column = mapping_row['Column']
                color = mapping_row['Color']
                # attribute = mapping_row['Attribute']
                attribute = mapping_row[self.COLUMN]
                if pd.notna(row[column]):
                    value = Esse.uniexcode(str(row[column]))
                    if attribute == 'id':
                        value = self.id(value)
                    node_dict[attribute] = value
            if node_dict:
                node_dict['color'] = color
                node_dicts.append(node_dict)
        return node_dicts

    def nodes(self, file_name, mappings):
        Esse.printf("____________Force: nodes {file_name}____________\n", RED, file_name=file_name)
        Dict.printf(mappings)
        seen = set()
        data = self.data
        Esse.printf("\nDATA\n{data}", RED, data=data)
        # code = ""
        # code = [self.node(index, series, mappings) for index, series in data.iterrows()]
        code = [item for index, row in data.iterrows() for item in self.node(index, row, mappings)]
        # printf uses code in formatted_text so gets confused to pass in just code
        dict_list = code
        Esse.printf("NODES\n{dict_list}", GREEN, dict_list=dict_list)        
        code = [d for d in code if 'id' in d and d['id'] not in seen and not seen.add(d['id'])]
        return code

    def edge(self, index, series, mappings):
        # Esse.printf("____________Force: map____________\n", GREEN)

        edge_dicts = []
        
        for edge_name, edge_df in mappings.items():

            # name: label
            # desc: text
            # source: The id of the source node.
            # target: The id of the target node.
            # color: To specify the color of the link.
            # strength: To specify the strength of the link (which can affect the force simulation).
            edge_dict = {}
            source_node = edge_df['Source Node'].iloc[0]
            source_id = edge_df['Source ID'].iloc[0]
            target_node = edge_df['Target Node'].iloc[0]
            target_id = edge_df['Target ID'].iloc[0]
            relationship = edge_df['Relationship'].iloc[0]
            
            #Esse.printf("Edge: source_id: {source_id} relationship: {relationship} target_id: {target_id}", BLUE, source_id=source_id,relationship=relationship,target_id=target_id)
            
            if not pd.isna(series[source_id]) and not pd.isna(series[target_id]):            
                edge_dict['name'] = relationship
                edge_dict['source'] = self.id(str(series[source_id]))
                edge_dict['target'] = self.id(str(series[target_id]))
 
            if edge_dict:
                edge_dicts.append(edge_dict)
        return edge_dicts
        
    def edges(self, file_name, mappings):
        Esse.printf("____________Force: edges____________\n", GREEN, file_name=file_name)
        Dict.printf(mappings)
        data = self.data
        Esse.printf("\nDATA\n{data}", RED, data=data)
        code = ""
        code = [item for index, row in data.iterrows() for item in self.edge(index, row, mappings)]
        # printf uses code in formatted_text so gets confused to pass in just code
        dict_list = code
        Esse.printf("\nEDGES\n{dict_list}", BLUE, dict_list=dict_list)
        return code    

    def generate(self, node_dict, edge_dict, output_directory_path):
        directory = self.directory
        data_file_path = directory.data_file_path
        data = self.data
        file_name = self.domain.name.lower()
        Esse.printf("____________Force: generate____________\n", GREEN)
        Esse.printf("Data: {data_file_path}\n", YELLOW, data_file_path=data_file_path)       
        Esse.printf("\nDATA\n{data}", RED, data=data)
        Esse.printf("Dictionaries:\n", YELLOW)             
        Esse.printf("NODES\n{node_dict}", GREEN, node_dict=node_dict)
        Esse.printf("\nEDGES\n{edge_dict}", BLUE, edge_dict=edge_dict)

        #node_json = json.dumps(node_dict)
        #node_code = node_json
        #node_code = node_json.replace('\"id\":', 'id:').replace('\"name\":', 'name:').replace('\"desc\":', 'desc:').replace('\"color\":', 'color:')
        #node_code = node_code.replace(', ', ',')
        #edge_json = json.dumps(edge_dict)
        #edge_code = edge_json
        #edge_code = edge_json.replace('\"name\":', 'name:').replace('\"source\":', 'source:').replace('\"target\":', 'target:')
        #Esse.printf("\nCode:\n", YELLOW)          
        #Esse.printf("NODES\n{node_code}", GREEN, node_code=node_code)
        #Esse.printf("\nEDGES\n{edge_code}", BLUE, edge_code=edge_code)
        #ListFile(output_directory_path, "nodes").generate(str(node_code))
        #ListFile(output_directory_path, "edges").generate(str(edge_code))
        
        map_dict = {"nodes": node_dict, "links": edge_dict}
        #Esse.printf("\nCode\n{file_code}", RED, file_code=file_code)        
        DictFile(output_directory_path, file_name).generate(map_dict) 
        
# Resource for Domain (Graph or Platform)
class Resource(Esse):
    # Instantiate Platform or Graph    
    def __init__(self, manifestation):
        Esse.printf("____________Resource____________\n", GREEN)  
        manifestation.resource = self
        self.parent = manifestation      
        self.inspect()
                
# Platform Meta Class
class Platform(Resource):
    def __init__(self, domain, transformer):
        Esse.printf("____________Platform____________\n", GREEN)              
        super().__init__(domain, transformer)
                        
    # Generate Platform
    def generate(self):
        Esse.printf("____________Platform: Generate____________\n", GREEN)             

# Graph Meta Class
class Graph(Resource):
    def __init__(self, manifestation):
        Esse.printf("____________Graph____________\n", GREEN)
        super().__init__(manifestation)

    # Transform given Aspect of the Mapping (Nodes, Edges)
    def transform(self, aspect):
        Esse.printf("____________Graph: Transform {aspect}____________\n", GREEN, aspect=aspect)
        # Transform the Source Data (Spreadsheet) into this Resource (Graph)
        self.inspect()
        # Get the Domain for this Transformation
        domain = self.domain
        domain.inspect()
        # Get the Transformation Mapping from the Domain
        mapping = domain.mapping
        mapping.inspect()
        # Get the Transformation Mappings from the Mapping Dictionary
        mappings = mapping.mapping[aspect]
        DF.printf(aspect, mappings)
        # Get the Directory for the Data from the Domain
        directory = domain.directory       
        directory.inspect()
        # Get the Data File Path from the Directory
        data_file_path = directory.data_file_path
        # Finally, get the Transformer Class (Cypher) as the Transformation
        transformer = self.transformer        
        transformation = transformer.get_method_name(aspect)
        Esse.printf("TRANSFORM: {transformation}", RED, transformation=transformation)
        code = ""   
        try:
            # Calls nodes or edges to get the code
            method = Esse.extricate(transformer, transformation)
            if not callable(method):
                raise ValueError(f"The method for transformation for '{transformation}' aspect does not exist.")
            code = method(data_file_path, mappings)
        except ValueError as ve:
            Esse.printf(f"ValueError: {ve}", RED)
            # Handle specific error, e.g., log, notify, etc.
            raise
        except Exception as e:
            Esse.printf(f"General Error during transformation: {e}", RED)
            # Log error and re-raise or handle accordingly
            raise
            
        return code
   
    # Generate Graph
    def generate(self, output_directory_path):
        Esse.printf("____________Graph: Generate____________\n", GREEN)       
        Esse.printf("\nOutput: {output_directory_path}\n", BLUE, output_directory_path=output_directory_path)        
        self.inspect()
        node_code = ""
        edge_code = ""
        domain = self.domain
        mapping = domain.mapping
        directory = domain.directory
        transformer = self.transformer
        data = transformer.data
        
        domain.inspect()        
        mapping.inspect()
        directory.inspect()
    
        node_code = self.transform("Nodes")
        edge_code = self.transform("Edges")
        
        df = mapping.df
        mappings = DF.split(df, 'Node')
        Dict.printf(mappings)
        
        mappings = DF.split(df, 'Relationship')
        Dict.printf(mappings)
        
        transformer.inspect()        
        generation = transformer.get_method_name(GENERATION)

        try:
            # Generate for Transformer (Cypher, Force)
            generate = Esse.extricate(transformer, generation)
            if not callable(generate):
                raise ValueError(f"The generation method: '{generation}' does not exist.")
            generate(node_code, edge_code, output_directory_path)
        except ValueError as ve:
            Esse.printf(f"ValueError: {ve}", RED)
            # Handle specific error, e.g., log, notify, etc.
            raise
        except Exception as e:
            Esse.printf(f"General Error during transformation: {e}", RED)
            # Log error and re-raise or handle accordingly
            raise
            
# CSV File convenience class
class CSVFile(File):
    def __init__(self, path):
        super().__init__(path)
        self.encoding = self.set_encoding()
        
    def ingest(self, encoding=DEFAULT_ENCODING):
        path = self.path
        df = pd.read_csv(self.path, dtype=str, encoding=encoding)
        return df
    
# Data File classes
class DataFile(CSVFile):
    #Initialize Data File calls File Initialize
    def __init__(self, path):
        super().__init__(path)
        
    # Find the Mappinging pattern matching the current file
    def get_file_matching_pattern(self, mapping, phase, aspect):
        pith = self.pith
        aspect_mapping_patterns = mapping.get_aspect_mapping_patterns(phase, aspect)
        
        return next((pattern for pattern in aspect_mapping_patterns if fnmatch.fnmatch(path, pattern)), None)
        
# Mapping File from which Mapping Data Frame is extracted
class MappingFile(CSVFile):
    def __init__(self, mapping):
        Esse.printf("____________MappingFile:____________\n", GREEN,)          

        # Manifestation <- Domain <- Mapping <- MappingFile          
        mapping.file = self
        self.parent = mapping

        # Manifestation <- Domain <- Mapping <- MappingFile  
        # Manifestation -> Directory         
        directory = mapping.parent.parent.directory
        
        # ../domain/trees        
        domain_path = directory.path
        
        mapping_path = os.path.join(domain_path, MAPPING_PITH)             
        mapping_file_path = File.find_matching_file(mapping_path, MAPPING_PITH)
        super().__init__(mapping_file_path) 
        Esse.printf("Mapping File: {mapping_file_path}", YELLOW, mapping_file_path=mapping_file_path)

    # Process MappingFile and return Data Frame
    def process(self, mapping):
        Esse.printf("____________MappingFile: Process____________\n", GREEN)          
        path = self.path
        base = self.base
        pith = self.pith
        encoding = self.encoding
        
        Esse.printf("    Encoding: {encoding} Path: {path} Base: {base} Pith: {pith}", YELLOW, encoding=encoding, path=path, base=base, pith=pith)
        
        # Ingest file through parent CSVFile convenience class
        
        df = self.ingest()
        # tranformer_class = self.get_class(transformation)
        # Reason for this is am dropping blank values in mapping column for this class in the CSV:
        # e.g. only id and common_name in the Column column are being mapped in the Attribute column (for Force Transformer)
        # height and diameter are not but they are in the Property column (for Cyper Transformer)
        # but is the encapsulation of this specification to be asserted in the "Code Graph" or remain in the class or simply handled in Mapping? 
        #transformer_column = self.get_constant(tranformer_class, 'COLUMN')
        domain = mapping.parent
        transformer = domain.parent.transformer

        # TODO: Obviate or obliterate
        transformer_column = transformer.COLUMN
        df_reduced = df.dropna(subset=[transformer_column])
        
        return df_reduced

# Mapping class itself
class Mapping(Esse):
    # Initialize Mapping from Directory through Data Frame
    # name, df, Nodes, Edges
    def __init__(self, domain):
        Esse.printf("____________Mapping {domain_name} {directory}____________\n", GREEN, domain_name=domain_name, directory=directory)

        # Manifestation <- Domain <- Mapping      
        domain.mapping = self
        self.parent = domain
        
        # Manifestation <- Domain <- Mapping <- MappingFile
        mapping_file = MappingFile(self)
        self.file = mapping_file
        
        # Mapping Structures
        self.mapping = {}
        self.mapping.update({'Nodes': {}})
        self.mapping.update({'Edges': {}})

        mapping_df = mapping_file.process(self)
                
        # Pattern is that Child will set itself and Parent        

        domain_name = domain.name
        
        directory = domain.parent.directory

        self.df = mapping_df
        
        DF.printf(domain_name, mapping_df)
        
        # Just split the Mapping up into Nodes and Edges - don't process the data.  
        # TODO: Combine?
        self.process()
    
    # Process Data Frame for Mapping by splitting those with all Nodes and only those with Relationships
    def process(self):        
        Esse.printf("\n____________Mapping Process____________\n", GREEN)
        
        df = self.df
        mappings = DF.split(df, 'Node')
        Dict.printf(mappings)
        
        self.mapping.update({'Nodes': mappings})
        
        mappings = DF.split(df, 'Relationship')
        Dict.printf(mappings)
        
        self.mapping.update({'Edges': mappings})        

# YAML File convenience class
class YAMLFile(File):
    def __init__(self, path):
        super().__init__(path)

    def ingest(self):
        path = self.path
        with open(path, 'r') as yaml_file:
            yo = yaml.safe_load(yaml_file)
            
        return yo
        
# Model File from which all Domain configuration parameters are defined.
class ModelFile(YAMLFile):
    def __init__(self, model):
        Esse.printf("____________ModelFile:____________\n", GREEN)          
    
        # Manifestation <- Domain <- Model <- ModelFile          
        model.file = self
        self.parent = model

        # Manifestation <- Domain <- Model <- ModelFile
        # Manifestation -> Directory         
        directory = model.parent.parent.directory

        # ../domain/trees        
        domain_path = directory.path
        
        # ../domain/trees/yaml      
        model_path = os.path.join(domain_path, MODEL_PITH)             
        model_file_path = File.find_matching_file(model_path, MODEL_PITH)
        super().__init__(model_file_path) 
        Esse.printf("Model File: {model_file_path}", YELLOW, model_file_path=model_file_path)    

# Model class
class Model(Esse):
    # Initialize Model from Directory through Data Frame
    # name, df, Nodes, Edges
    def __init__(self, domain):
        Esse.printf("____________Model____________\n", GREEN)

        # Manifestation <- Domain <- Model      
        domain.model = self
        self.parent = domain

        # Manifestation <- Domain <- Model <- ModelFile
        model_file = ModelFile(self)
        self.file = model_file
        
        yo = model_file.ingest()
        self.yo = yo
        # --------------------------------------------------        
        # TODO: Nodes, Colors      
        # --------------------------------------------------     
        self.nodes = {}
        self.nodes.update({'Nodes': yo['nodes'])
        ]})

# Domain Generating Graph
class Domain(Esse):
    # Instantiate Domain, adding both Directory and Mapping
    def __init__(self, manifestation):
        Esse.printf("____________Domain____________\n", GREEN)          

        # Manifestation <- Domain
        manifestation.domain = self
        self.parent = manifestation
        
        self.name = manifestation.name
        self.path = manifestation.path

        self.manifest = Manifest(self)

        # Manifestation <- Domain <- Model            
        self.model = Model(self)
        # Manifestation <- Domain <- Mapping      
        self.mapping = Mapping(self)
        
# Directory Class
class Directory(File):
    # Initialize Directory with code and data subdirectories
    # parent trees
    def __init__(self, manifestation):
        Esse.printf("____________Directory____________\n", GREEN)

        # Manifestation <- Directory 
        manifestation.directory = self
        self.parent = manifestation
        
        # name: trees
        domain_name = manifestation.name

        # path: ../domain/trees
        domain_path = manifestation.path
        
        # Manifestation <- Directory <- Manifest
        manifest = Manifest(self)

        # TODO: source/target? extension? file_type?
        # INPUT_PITH = "csv"
        # OUTPUT_PITH = "json"
        # DATA_PITH = "data"
        
        
        # -------------------------------------------------
        # manifest_directory, model_directory, mapping_directory
        # -------------------------------------------------
                
        self.input_directory_pith = manifest.input_pith
        self.data_directory_pith = manifest.data_pith
        self.output_directory_pith = manifest.output_pith   

        # TODO: embed output within transformer
        # ../domain/csv
        input_directory_path = self.instantiate(self.directory_path, self.input_directory_pith)
        self.input_directory_path = input_directory_path
        # ../domain/csv/data
        data_directory_path = self.instantiate(self.input_directory_path, self.data_directory_pith)
        self.data_directory_path = data_directory_path
        # ../domain/csv/json
        output_directory_path = self.instantiate(self.directory, self.output_directory_pith)
        self.output_directory_path = output_directory_path

        Esse.printf("Path: {path} Input: {input_directory_path} Data: {data_directory_path} Output: {output_directory_path}", YELLOW, path=path, data_directory_path=data_directory_path, input_directory_path=input_directory_path, output_directory_path=output_directory_path)
        # TODO: Multiple Files
        # --------------------------------------------------  
        # Multiple Files...
        # Exceptional_Trees_On_Oahu.csv
        #data_file_name = source  + "." + INPUT_PITH
        #self.data_file_name = data_file_name
        
        # ../domain/csv/data/Exceptional_Trees.csv
        #data_file_path = os.path.join(data_directory_path, data_file_name)
        #self.data_file_path = data_file_path
        # --------------------------------------------------    
    # Instantiate subdirectory, creating if necessary
    
    def instantiate(self, directory, pith, create=False):
        path = os.path.join(directory, pith)
        os.makedirs(path, exist_ok=True) if create else None
        return path
    
    # Process the Mapping across all files for the current Phase
    def process(self):
        directory = self.path
        
        Esse.printf("\n____________directory_process____________\n", GREEN)                
        
        mapping = Mapping(directory)
        phase = self.base
        mapping.process(phase)
        
        data_directory = os.path.join(directory, "data")
        data_files = sorted(glob.glob(os.path.join(data_directory, "*.csv")))

        [DataFile(file).process(phase, mapping) for file in data_files]

# Manifest File from which all configuration parameters are defined.
class ManifestFile(YAMLFile):
    def __init__(self, manifest):
        Esse.printf("____________ManifestFile:____________\n", GREEN)
                
        # Manifestation <- Directory <- Manifest <- ManifestFile
        manifest.file = self
        self.parent = manifest
        
        # Manifestation <- Directory <- Manifest 
        directory_path = manifest.path
        
        # ../domain/trees/yaml
        manifest_path = os.path.join(directory_path, CONFIG_PITH)       
        manifest_file_path = File.find_matching_file(manifest_path, MANIFEST_PITH)
        super().__init__(manifest_file_path)         
        Esse.printf("Manifest File: {manifest_file_path}", YELLOW, manifest_file_path=manifest_file_path)

# Manifest class
class Manifest(Esse):
    # Initialize Manifest from Directory through Data Frame
    # name, df, Nodes, Edges
    def __init__(self, domain):
        Esse.printf("____________Manifest {domain_name} {directory}____________\n", GREEN, domain_name=domain_name, directory=directory)

        # Manifestation <- Directory <- Manifest      
        domain.manifest = self
        self.parent = domain
        self.path = domain.path
        
        # Manifestation <- Directory <- Manifest <- ManifestFile
        manifest_file = ManifestFile(self)
        self.file = manifest_file
        
        # YAML Object
        yo = manifest_file.ingest()
        self.yo = yo
        # --------------------------------------------------        
        # TODO: source/target? extension? file_type?
        # INPUT_PITH = "csv"
        # OUTPUT_PITH = "json"
        # DATA_PITH = "data"
        # source: csv
        # target: json
        # input: data
        # output: yada
        self.input_pith = yo['source']
        self.output_pith = = yo['target']
        self.data_pith = yo['input']
        self.yada_pith = yo['output']   
        # --------------------------------------------------            

class Manifestation(Esse):
    def __init__(self, domain_name):
        Esse.printf("____________Manifest____________\n", GREEN)

        self.name = domain_name
        self.path = ROOT_PATH
    
        # Pattern is that Child will set itself and Parent
        # Manifestation <- ...

        # Manifest
        manifest = Manifest(self)

        # Domain
        domain = Domain(self)
        
        # Transformer
        transformer = Esse.instantiate(transformer_class, self)
        
        # Resource
        resource = Esse.instantiate(resource_class, self)

        # Generate
        generate = Esse.extricate(resource, GENERATION) # generate method for Graph or Platform    

        output_directory_path = domain.directory.output_directory_path
        generate(output_directory_path)
    
# Create Domain and Generate Resource (Platform or Graph) and specific type (NeoModel for Platform or Cypher for Graph;)
def main(domain_name, resource_class, transformer_class, source):
    Esse.printf("____________Main____________\n", GREEN)         
    Esse.printf("Domain: {domain_name}  Resource: {resource_class}  Transformation: {transformer_class} Source: {source}", YELLOW, domain_name=domain_name, resource_class=resource_class, transformer_class=transformer_class, source=source)

    Manifestation(domain_name)

if __name__ == '__main__':
    
    Esse.printf("BEGIN...\n", RED)
    parser = argparse.ArgumentParser()
    parser.add_argument('domain_name', help="Domain")
    parser.add_argument('resource_class', help="Resource Class")
    parser.add_argument('transformer_class', help="Transformer Class")
    parser.add_argument('source', help="Source")  
    args = parser.parse_args()
    main(args.domain_name, args.resource_class, args.transformer_class, args.source)
    Esse.printf("\n...END", RED)

# python3 manifest.py trees
# python3 manifest.py trees Graph Cypher Exceptional_Trees_On_Oahu
# python3 manifest.py trees Graph Force Exceptional_Trees_On_Oahu



      



        
