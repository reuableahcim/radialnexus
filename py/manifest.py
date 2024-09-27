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

# Root paths and set types for configuration and mapping files
ROOT_PATH = "../"
DOMAIN_PITH = "domain"
MANIFEST_TYPE = "yaml"
MODEL_TYPE = "yaml"
MAPPING_TYPE = "csv"

# Configuration piths for software and the domain
MANIFEST_PITH = "manifest"
MODEL_PITH = "Model"
MAPPING_PITH = "Mapping"

# Placeholder constants for software mapping using csv file and a single transformation method
SOURCE_COLUMN = "Column"
GENERATION_METHOD = "generate"

# Constants for file analysis
SAMPLE_INPUT_SIZE = 10000
DEFAULT_ENCODING = 'UTF-8'

# Default Colors for Edges
EDGE_COLOR = 'CCCCCC'
EDGE_STRENGTH = '1024'

# Colors for printing
RED = "31;1"
GREEN = "32;1"
YELLOW = "33;1"
BLUE = "34;1"

# Meta class for all classes
class Esse:  
    # Print Formatted String with Keywords
    @staticmethod
    def printf(formatted_text, color, **kwargs):
        # TODO: Conditional?
        print(f"\033[{color}m{formatted_text.format(**kwargs)}\033[0m")

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

    @staticmethod
    def uniexcode(source_str):
        # Using unicodedata
        # nfkd_form = unicodedata.normalize('NFKD', source_str)
        # normalized_form = ''.join([c for c in nfkd_form if not unicodedata.combining(c)])
        # Using unidecode
        normalized_form = source_str
        normalized_form = unidecode(source_str)
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
        Esse.printf("\nDictionary:", RED)
        # print(json.dumps(dictionary, indent=4))
        formatted_output = '\n'.join(f"{key}: {value}" for key, value in dictionary.items())
        #Esse.printf("\n", RED)
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
        # Esse.printf("____________extract_row____________\n", YELLOW)
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
    def find_matching_file(directory_path, substring, extension=MAPPING_TYPE):
        # Esse.printf("Find: {substring} Exension: {extension}", YELLOW, substring=substring, extension=extension)    
        files = glob.glob(os.path.join(directory_path, f"*{substring}.{extension}"))
        return files[0] if files else None

# Directory Class
class Directory(File):
    # Initialize Directory with code and data subdirectories
    # parent trees
    # Instantiate subdirectory, creating if necessary
    def __init__(self, directory, pith, create=False):
        path = os.path.join(directory, pith)
        super().__init__(path)               
        os.makedirs(path, exist_ok=True) if create else None
    
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

# CSV File convenience class
class CSVFile(File):
    def __init__(self, path):
        super().__init__(path)
        self.encoding = self.set_encoding()
        
    def ingest(self, encoding=DEFAULT_ENCODING):
        path = self.path
        df = pd.read_csv(path, dtype=str, encoding=encoding)
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
     
# Dictionary Files    
class DictFile(File):
    def __init__(self, path):
        # Don't have to create path here from directory and name as path set in Manifest and being passed here.
        # dict_file_path = dict_file_directory  + "/" + dict_file_name + "." + OUTPUT_PITH
        # self.pith = dict_file_name
        # self.path = dict_file_path
        super().__init__(path)        

    def generate(self, map_dict):
        pith = self.pith
        dict_file_path = self.path
        Esse.printf("____________DictFile: Generate {pith}: {dict_file_path}____________\n", GREEN, pith=pith, dict_file_path=dict_file_path)  
        Esse.printf("\n\n{map_dict}", RED, map_dict=map_dict)
        with open(dict_file_path, 'w') as  dict_file:
            Esse.printf("Generating...", RED)
            json.dump(map_dict, dict_file, indent=4)
            Esse.printf("...done", RED) 
        
# Transformer for Domain (Cypher or NeoModel)
class Transformer(Esse):
    # Now extracted from the manifest
    # COLUMN = 'Property'

    def __init__(self, manifestation):
        Esse.printf("____________Transformer____________\n", GREEN)                
        # Set the transformer in the manifestation     
        manifestation.transformer = self
        self.manifestation = manifestation
                
# NeoModel Transformer Class
class NeoModel(Transformer):
    def __init__(self, domain):
        Esse.printf("____________NeoModel____________\n", GREEN)                
        super().__init__(domain)

# 3D-Force Graph Transformer Class
class Force(Transformer):
    IDS = set()
    
    def __init__(self, manifestation):
        super().__init__(manifestation)
        Esse.printf("____________Force____________\n", GREEN)
        # TODO: Multiple Source Files and Abstract Types for Target Files

    def id(self, value):
        value = value.replace(',','')
        return value

    def new_id(self, i):
        # Check if the ID has already been seen
        if i in self.IDS:
            return False  # Return None if the ID has been seen before
        else:
            # Add the new ID to the set and return the ID
            self.IDS.add(i)
            return True
                
    # def id(self, value):
    #     # Still not sure why was doing this...
    #     # value = value.replace(',','')
    #     i = value
    #     if i in self.IDS:
    #         return None  # Return None if the ID has been seen before
    #     else:
    #         # Add the new ID to the set and return the ID
    #         self.IDS.add(i)
    #         return i  
        
    def node(self, index, row, mappings):
        Esse.printf("____________Force: node____________\n", GREEN)
        # id: A unique identifier for the node (required).
        # name: A display name for the node.
        # color: To specify the node color.
        # val: Another way to define the node's volume (often affects visual size).
        # desc: text
        # size: To specify the size of the node
        # target_column is the column in the mapping file to reference (Attribute for Force, Property for Cypher)
        manifestation = self.manifestation
        manifest = manifestation.manifest
        target_column = manifest.target_column
        model = manifestation.domain.model
        
        node_dicts = []
        for node_name, node_df in mappings.items():
            node_dict = {}
            #color = '95CDEA'
            Esse.printf("\n{node_name}", RED, node_name=node_name)
            for _, mapping_row in node_df.iterrows():
                # Always Column (CSV File)
                column = mapping_row[SOURCE_COLUMN]
                if pd.notna(row[column]):
                    name = mapping_row['Node']
                    attribute = mapping_row[target_column]                
                    value = Esse.uniexcode(str(row[column]))
                    node_dict[attribute] = value
                    color = model.nodes['Nodes'][name]['color']
                    node_dict['color'] = color
                    Esse.printf("{name}: {attribute} ({value})", BLUE, name=name, attribute=attribute, value=value)
            if node_dict:
                node_id = node_dict['id']
                if self.new_id(node_id):
                    Esse.printf("{node_id}: {node_dict}\n", BLUE, node_id=node_id, node_dict=node_dict)
                    node_dicts.append(node_dict)
                else:
                    Esse.printf("{node_id}: SEEN\n", YELLOW, node_id=node_id)
                     
        return node_dicts

    # Integrant called from Graph transform method for aspect: nodes
    def nodes(self, mappings):
        Esse.printf("____________Force: nodes____________\n", RED)
        Dict.printf(mappings)
        manifestation = self.manifestation
        data = manifestation.data
        seen = set()
        Esse.printf("\nDATA\n{data}", RED, data=data)
        # code = ""
        # code = [self.node(index, series, mappings) for index, series in data.iterrows()]
        code = [item for index, row in data.iterrows() for item in self.node(index, row, mappings)]
        # printf uses code in formatted_text so gets confused to pass in just code
        # dict_list = code
        Esse.printf("NODES 1\n{code}", GREEN, code=code)        
        # code = [d for d in code if 'id' in d and d['id'] not in seen and not seen.add(d['id'])]
        # Esse.printf("NODES 2\n{code}", GREEN, code=code) 
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
                        
            if not pd.isna(series[source_id]) and not pd.isna(series[target_id]):            
                edge_dict['name'] = relationship
                # OK, not sure but necessary otherwise json.dump will spit out null values
                edge_dict['source'] = self.id(str(series[source_id]))
                edge_dict['target'] = self.id(str(series[target_id]))           
                edge_dict['color'] = EDGE_COLOR
                edge_dict['strength'] = EDGE_STRENGTH
 
            if edge_dict:
                edge_dicts.append(edge_dict)
        return edge_dicts

    # Integrant called from Graph transform method for aspect: edges        
    def edges(self, mappings):
        Esse.printf("____________Force: edges____________\n", GREEN)
        Dict.printf(mappings)
        manifestation = self.manifestation
        data = manifestation.data
        Esse.printf("\nDATA\n{data}", RED, data=data)
        code = ""
        code = [item for index, row in data.iterrows() for item in self.edge(index, row, mappings)]
        # dict_list = code
        # Esse.printf("\nEDGES\n{dict_list}", BLUE, dict_list=dict_list)
        Esse.printf("\nEDGES\n{code}", BLUE, code=code)
        return code    

    def generate(self, node_dict, edge_dict):
        manifestation = self.manifestation
        manifest = manifestation.manifest
        data = manifestation.data
               
        source_file_path = manifest.source_file_path
        target_file_path = manifest.target_file_path
        
        Esse.printf("____________Force: generate____________\n", GREEN)
        Esse.printf("Source: {source_file_path}\n", YELLOW, source_file_path=source_file_path)       
        Esse.printf("\nDATA\n{data}", RED, data=data)
        Esse.printf("Dictionaries:\n", YELLOW)             
        Esse.printf("NODES\n{node_dict}", GREEN, node_dict=node_dict)
        Esse.printf("\nEDGES\n{edge_dict}", BLUE, edge_dict=edge_dict)
        Esse.printf("Target: {target_file_path}\n", YELLOW, target_file_path=target_file_path)       
        
        map_dict = {"nodes": node_dict, "links": edge_dict}     
        # DictFile(target_file_path, file_name).generate(map_dict) 
        DictFile(target_file_path).generate(map_dict)
                   
# Cypher Transformer Class
class Cypher(Transformer):

    def __init__(self, manifestation):
        super().__init__(manifestation)
        Esse.printf("____________Cypher____________\n", YELLOW)
        manifest = manifestation.manifest
        # Copy down the template_file_path and create the Script file object from the target_file_path
        template_file_path = manifest.template_file_path
        target_file_path = manifest.target_file_path
        self.template_file_path = template_file_path
        script = Script(target_file_path)
        self.script = script
        
    def get_transform(self, series, column_column):
        column = series[column_column]
        code = f"row.{column}"
        return code
        
    def node_assignments(self, variable_name, node_df):
        Esse.printf("____________Cypher: node_assignment {variable_name}____________\n", YELLOW, variable_name=variable_name)
        manifestation = self.manifestation
        manifest = manifestation.manifest
        target_column = manifest.target_column
                
        code = ""
        if not node_df.empty:
            code += "".join([f"\t\tSET {variable_name}.{row[target_column]} = row.{row[SOURCE_COLUMN]}\n" for index, row in node_df.iterrows()])
        Esse.printf("{code}", BLUE, code=code)
        return code
    
    def node_set(self, node_name, node_df):
        Esse.printf("____________Cypher: node_set {node_name}____________\n", YELLOW, node_name=node_name)
        DF.printf(node_name, node_df)
        variable_name = self.get_variable_name(node_name)
        code = ""
        if node_df is not None:
            code += self.node_assignments(variable_name, node_df)
        if code.endswith(","):
            code = code.rstrip(",")
        code += "\n"
        Esse.printf("{code}", BLUE, code=code)
        return code
    
    def node_merge(self, node_name, id_series):
        Esse.printf("____________Cypher: node_merge {node_name}____________\n", YELLOW, node_name=node_name)
        manifestation = self.manifestation
        manifest = manifestation.manifest
        target_column = manifest.target_column
        source_column = SOURCE_COLUMN
        variable_name = self.get_variable_name(node_name)
        code = ""
        if not id_series.empty:
            #Esse.printf("Series: {id_series}\nSource Column: {source_column} Target Column:{target_column}", RED, id_series=id_series, source_column=source_column, target_column=target_column)
            identifier_transform = self.get_transform(id_series, source_column)
            code += f"\tWITH row\n"
            code += f"\tWHERE {identifier_transform} IS NOT NULL\n"       
            code += f"\tMERGE ({variable_name}:{node_name} "
            code += f"{{id:toString({identifier_transform})}}"   
            code += ")\n"  
        Esse.printf("{code}", BLUE, code=code)
        return code
    
    def node_body(self, node_name, node_df):
        Esse.printf("____________Cypher: node_body {node_name}____________\n", YELLOW, node_name=node_name)
        manifestation = self.manifestation
        manifest = manifestation.manifest
        target_column = manifest.target_column
        id_property = manifest.id_property
        
        code = ""
        id_series, redux_df = DF.extract_series(node_name, node_df, target_column, id_property)
        code += self.node_merge(node_name, id_series)
        code += self.node_set(node_name, redux_df)
        Esse.printf("{code}", BLUE, code=code)
        return code

    def node_header(self):
        manifestation = self.manifestation
        manifest = manifestation.manifest

        source_file_path = manifest.source_file_path      
        code = ""
        code += f"\n# Loading Nodes from {source_file_path}"
        code += f"\nquery = '''\n"
        code += f"LOAD CSV WITH HEADERS FROM \"file:///{source_file_path}\" AS row\n"
        Esse.printf("{code}", BLUE, code=code)
        return code

    def node_footer(self):
        code = ""
        code += f"'''"
        code += f"\ngraph.run(query)"
        code += f"\n"
        return code

    def node(self, node_name, node_df):
        Esse.printf("____________Cypher: node {node_name}____________\n", YELLOW, node_name=node_name)
        code = ""
        code += self.node_header()
        code += self.node_body(node_name, node_df)
        code += self.node_footer()
        Esse.printf("{code}", BLUE, code=code)
        return code
    
    # Integrant called from Graph transform method for aspect: nodes
    def nodes(self, mappings):
        Esse.printf("____________Cypher: nodes____________\n", YELLOW)
        code = ""
        code += "".join([self.node(node_name, node_df) for node_name, node_df in mappings.items()])
        Esse.printf("{code}", BLUE, code=code)
        return code

    def edge_line(self, series):
        Esse.printf("____________Cypher: edge_line____________\n", YELLOW)  
        code = ""
        
        source_variable = "source"
        source_node = series['Source Node']
        source_identity = series['Source ID']
        target_variable = "target"
        target_node = series['Target Node']
        target_identity = series['Target ID']
        relationship = series['Relationship']

        code += "\tMATCH "
        code += f"({source_variable}:{source_node} {{id: toString(row.{source_identity})}}), ({target_variable}:{target_node} {{id: toString(row.{target_identity})}})\n"
        code += "\t\tMERGE"
        code += f"({source_variable})-[edge:{relationship}]->({target_variable})\n"

        Esse.printf("{code}", BLUE, code=code)
        return code

    def edge_body(self, edge_name, edge_df):
        Esse.printf("____________Cypher: edge_body {edge_name}____________\n", YELLOW, edge_name=edge_name)
        code = ""  
        code += "".join([self.edge_line(series) for _, series in edge_df.iterrows()])
        Esse.printf("{code}", BLUE, code=code)
        return code

    def edge_header(self):
        manifestation = self.manifestation
        manifest = manifestation.manifest  
        source_file_path = manifest.source_file_path  
        
        code = ""
        code += f"\n# Loading Relationships from {source_file_path}"
        code += f"\nquery = '''\n"
        code += f"LOAD CSV WITH HEADERS FROM \"file:///{source_file_path}\" AS row\n"
        Esse.printf("{code}", BLUE, code=code)
        return code

    def edge_footer(self):
        code = ""
        code += f"'''"
        code += f"\ngraph.run(query)"
        code += f"\n"
        Esse.printf("{code}", BLUE, code=code)
        return code

    def edge(self, edge_name, edge_df):
        Esse.printf("____________Cypher: edge____________\n", YELLOW)
        code = ""
        code += self.edge_header()
        code += self.edge_body(edge_name, edge_df)
        code += self.edge_footer()
        Esse.printf("{code}", BLUE, code=code)
        return code

    # Integrant called from Graph transform method for aspect: edges
    def edges(self, mappings):
        Esse.printf("____________Cypher: edges____________\n", YELLOW)
        Dict.printf(mappings)
        code = ""
        code += "".join([self.edge(edge_name, edge_df) for edge_name, edge_df in mappings.items()])
        Esse.printf("{code}", BLUE, code=code)
        return code

    def generate(self, node_code, edge_code):
        template_file_path = self.template_file_path
        script = self.script
        code = node_code + edge_code        
        script.generate(template_file_path, code)

# Resource for Domain (Graph or Platform)
class Resource(Esse):
    # Instantiate Platform or Graph    
    def __init__(self, manifestation):
        Esse.printf("____________Resource____________\n", GREEN)
        # Set the resource in the manifestation
        manifestation.resource = self
        self.manifestation = manifestation
                
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
        super().__init__(manifestation)
        Esse.printf("____________Graph____________\n", YELLOW)

    # Transform given Aspect of the Mapping (Nodes, Edges)
    def transform(self, aspect):
        Esse.printf("____________Graph: Transform {aspect}____________\n", YELLOW, aspect=aspect)
        # Transform the Source Data (Spreadsheet) into this Resource (Graph)
        # self.inspect()
        # Get the Domain for this Transformation
        manifestation = self.manifestation
        domain = manifestation.domain
        # domain.inspect()
        # Get the Transformation Mapping from the Domain
        mapping = domain.mapping
        # mapping.inspect()
        # Get the Transformation Mappings from the Mapping Dictionary
        mappings = mapping.mapping[aspect]
        DF.printf(aspect, mappings)
        # Get the Directory for the Data from the Domain
        # directory = domain.directory       
        # directory.inspect()
        # Get the Data File Path from the Directory
        # data_file_path = directory.data_file_path
        # Finally, get the Transformer Class (Cypher) as the Transformation
        transformer = manifestation.transformer      
        transformation = transformer.get_method_name(aspect)
        Esse.printf("TRANSFORM: {transformation}\n", RED, transformation=transformation)
        code = ""   
        try:
            # The nodes or edges instantiated in the appropriate code form for the transformation
            integrant = Esse.extricate(transformer, transformation)
            if not callable(integrant):
                raise ValueError(f"The integrant to instantiation the '{transformation}' aspect does not exist.")
            # code = method(data_file_path, mappings)
            code = integrant(mappings)            
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
    def generate(self):
        Esse.printf("____________Graph: Generate____________\n", GREEN)       
        self.inspect()
        manifestation = self.manifestation
        manifest = manifestation.manifest
        data = manifestation.data        
        domain = manifestation.domain
        mapping = domain.mapping
        transformer = manifestation.transformer

        Esse.printf("Components:\n", YELLOW)        
        domain.inspect()        
        mapping.inspect()
        transformer.inspect()    
                
        df = mapping.df
        
        Esse.printf("BEGIN: NODE MAPPING...\n", YELLOW)
        node_code = self.transform("Nodes")
        node_mappings = DF.split(df, 'Node')
        Dict.printf(node_mappings)
        Esse.printf("\nDONE: NODE MAPPING.\n", YELLOW)
        
        Esse.printf("BEGIN: EDGE MAPPING...\n", YELLOW)
        edge_code = self.transform("Edges")              
        edge_mappings = DF.split(df, 'Relationship')
        Dict.printf(edge_mappings)
        Esse.printf("\nDONE: EDGE MAPPING.\n", YELLOW)
                
        generation = transformer.get_method_name(GENERATION_METHOD)

        try:
            # Generate Method for specific Transformer (Cypher, Force)
            generate = Esse.extricate(transformer, generation)
            if not callable(generate):
                raise ValueError(f"The generation method: '{generation}' does not exist.")
            # Force.generate
            generate(node_code, edge_code)
        except ValueError as ve:
            Esse.printf(f"ValueError: {ve}", RED)
            # Handle specific error, e.g., log, notify, etc.
            raise
        except Exception as e:
            Esse.printf(f"General Error during transformation: {e}", RED)
            # Log error and re-raise or handle accordingly
            raise
               
# Mapping File from which Mapping Data Frame is extracted
class MappingFile(CSVFile):
    def __init__(self, mapping):
        Esse.printf("____________MappingFile:____________\n", GREEN,)          

        # Manifestation <- Domain <- Mapping <- MappingFile          
        mapping.file = self
        self.mapping = mapping

        # ../domain/trees/csv 
        directory_path = mapping.directory_path
                   
        mapping_file_path = File.find_matching_file(directory_path, MAPPING_PITH, MAPPING_TYPE)
        super().__init__(mapping_file_path) 
        Esse.printf("Mapping File: {mapping_file_path}\n", BLUE, mapping_file_path=mapping_file_path)

    # Process MappingFile and return Data Frame
    def process(self, mapping):
        Esse.printf("____________MappingFile: Process____________\n", GREEN)          
        path = self.path
        base = self.base
        pith = self.pith
        encoding = self.encoding

     
        Esse.printf("Encoding: {encoding} Path: {path} Base: {base} Pith: {pith}\n", YELLOW, encoding=encoding, path=path, base=base, pith=pith)
        
        # Ingest file through parent CSVFile convenience class       
        df = self.ingest()
        
        # transformer_column
        # Column used for mapping depends on the Transformer - Cypher uses Property, Force uses Attribute
        # Reason for this is am dropping blank values in mapping column for this class in the CSV:
        # e.g. only id and common_name in the Column column are being mapped in the Attribute column (for Force Transformer)
        # height and diameter are not but they are in the Property column (for Cyper Transformer)        
        transformer_column = mapping.transformer_column
        
        df_reduced = df.dropna(subset=[transformer_column])
        
        return df_reduced

# Mapping class itself
class Mapping(Esse):
    # Initialize Mapping from Directory through Data Frame
    # name, df, Nodes, Edges
    def __init__(self, domain):
        Esse.printf("____________Mapping____________\n", GREEN)

        # Manifestation <- Domain <- Mapping      
        domain.mapping = self
        self.domain = domain
        domain_name = domain.name        
        transformer_class = domain.transformer_class
        manifest = domain.manifest
        self.transformer_column = manifest.yo['transformer'][transformer_class]['column']

        # Paths
        # ../domain/trees/csv  
        self.directory_path = domain.mapping_path
         
        # Manifestation <- Domain <- Mapping <- MappingFile
        mapping_file = MappingFile(self)
        self.file = mapping_file
        
        # Mapping Structures
        self.mapping = {}
        self.mapping.update({'Nodes': {}})
        self.mapping.update({'Edges': {}})

        mapping_df = mapping_file.process(self)
        self.df = mapping_df   
        DF.printf(domain_name, mapping_df)
        
        # Just split the Mapping up into Nodes and Edges - don't process the data.  
        # TODO: Combine?
        self.process()
    
    # Process Data Frame for Mapping by splitting those with all Nodes and only those with Relationships
    def process(self):        
        Esse.printf("\n____________Mapping Process____________", GREEN)
        
        df = self.df
        mappings = DF.split(df, 'Node')
        Dict.printf(mappings)
        
        self.mapping.update({'Nodes': mappings})
        
        mappings = DF.split(df, 'Relationship')
        Dict.printf(mappings)
        
        self.mapping.update({'Edges': mappings})
        Esse.printf("\n", RED)

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
        self.model = model

        # Paths
        # ../domain/trees/yaml    
        directory_path = model.directory_path
        
        model_file_path = File.find_matching_file(directory_path, MODEL_PITH, MODEL_TYPE)
        super().__init__(model_file_path) 
        Esse.printf("Model File: {model_file_path}\n", BLUE, model_file_path=model_file_path)    

# Model class
class Model(Esse):
    # Initialize Model from Directory through Data Frame
    # name, df, Nodes, Edges
    def __init__(self, domain):
        Esse.printf("____________Model____________\n", GREEN)

        # Manifestation <- Domain <- Model      
        domain.model = self
        self.domain = domain
        
        # Paths
        # ../domain/trees/yaml        
        self.directory_path = domain.model_path
        
        # Manifestation <- Domain <- Model <- ModelFile
        model_file = ModelFile(self)
        self.file = model_file
        
        yo = model_file.ingest()
        self.yo = yo
        # --------------------------------------------------        
        # TODO: Nodes, Colors      
        # --------------------------------------------------     
        self.nodes = {}
        self.nodes.update({'Nodes': yo['nodes']})
        Esse.printf("{yo}\n", GREEN, yo=yo)

# Domain Generating Graph
class Domain(Esse):
    # Instantiate Domain, adding both Directory and Mapping
    def __init__(self, manifestation):
        Esse.printf("____________Domain____________\n", GREEN)          

        # Manifestation <- Domain
        manifestation.domain = self
        self.manifestation = manifestation
        self.name = manifestation.name
        
        # Transformer Class and Manifest
        self.transformer_class = manifestation.transformer_class
        self.manifest = manifestation.manifest
        
        # Paths
        self.directory_path = manifestation.domain_path
        self.model_path = os.path.join(self.directory_path, MODEL_TYPE)    
        self.mapping_path = os.path.join(self.directory_path, MAPPING_TYPE)
        
        # Manifestation <- Domain <- Model            
        self.model = Model(self)
        
        # Manifestation <- Domain <- Mapping      
        self.mapping = Mapping(self)
        
# Manifest File from which all configuration parameters are defined.
class ManifestFile(YAMLFile):
    def __init__(self, manifest):
        Esse.printf("____________ManifestFile:____________\n", GREEN)
                
        # Manifestation <- Manifest <- ManifestFile
        manifest.file = self
        self.manifest = manifest
        
        # ../yaml   
        directory_path = manifest.manifest_path
        
        manifest_file_path = File.find_matching_file(directory_path, MANIFEST_PITH, MANIFEST_TYPE)
        super().__init__(manifest_file_path)         
        Esse.printf("Manifest File: {manifest_file_path}\n", BLUE, manifest_file_path=manifest_file_path)

# Manifest class
class Manifest(Esse):
    # Initialize Manifest from Directory through Data Frame
    # name, df, Nodes, Edges
    def __init__(self, manifestation):
        Esse.printf("____________Manifest____________\n", GREEN)

        # Manifestation <- Manifest  
        manifestation.manifest = self
        self.manifestation = manifestation
        
        # Get the root directory path and the transformer class from the manifestation
        root_path = manifestation.root_path                  # ../
        domain_name = manifestation.name
        domain_path = manifestation.domain_path              # ../domain/trees
        transformer_class = manifestation.transformer_class  # Force
        source_file_pith = manifestation.source_file_pith    # Exceptional_Trees 

        # Set the manifest_path here
        # ../yaml
        self.manifest_path = os.path.join(root_path, MANIFEST_TYPE)
        
        # Get the manifest file
        # ../yaml/manifest.yaml
        manifest_file = ManifestFile(self)
        
        # Set the domain path as the main directory path
        # ../domain/trees
        self.directory_path = domain_path
        
        # YAML Object will set the directories and constants for the process
        # source: csv
        # target: json
        # input: data
        # output: yada
        # column: Attribute or Property for mapping
        # Assuming Force is the Transformer        
        yo = manifest_file.ingest()
        self.yo = yo

        self.source_pith = yo['transformer'][transformer_class]['source']    # csv
        self.target_pith = yo['transformer'][transformer_class]['target']   # json
        self.data_pith = yo['transformer'][transformer_class]['input']      # data
        self.yada_pith = yo['transformer'][transformer_class]['output']     # json
        self.target_column = yo['transformer'][transformer_class]['column'] # Attribute
        self.id_property = yo['transformer'][transformer_class]['id']

        # Directory Paths
        # ../domain/trees/csv
        source_directory = Directory(self.directory_path, self.source_pith)
        self.source_directory = source_directory
        # source_directory_path = self.instantiate(self.directory_path, self.source_pith)
        # self.source_directory_path = source_directory_path
        
        # ../domain/trees/csv/data
        # data_directory_path = self.instantiate(self.source_directory_path, self.data_pith)
        # self.data_directory_path = data_directory_path
        data_directory = Directory(source_directory.path, self.data_pith)
        self.data_directory = data_directory
        data_directory_path = data_directory.path
                                       
        # TODO: Multiple Source Files and Abstract Types for Target Files
        
        # SOURCE
        # File Names and Paths
        # Source - Using CSV as input so will set the DataFile(CSVFile) in the manifest
        # Exceptional_Trees_On_Oahu.csv
        source_file_name = source_file_pith + "." + self.source_pith
        self.source_file_name = source_file_name
        
        # ../domain/trees/csv/data/Exceptional_Trees_On_Oahu.csv
        source_file_path = os.path.join(data_directory_path, source_file_name)
        self.source_file_path = source_file_path
        
        source_file = DataFile(source_file_path)
        self.source_file = source_file
        
        # Ingest file - set the data in the manifestation, not the manifest.
        df = source_file.ingest()
        manifestation.data = df

        # TARGET
        # ../domain/trees/json
        # ../domain/trees/cy        
        # target_directory_path = self.instantiate(domain_path, self.target_directory_pith)
        # self.target_directory_path = target_directory_path
        target_directory = Directory(domain_path, self.target_pith)
        self.target_directory = target_directory
        target_directory_path = target_directory.path
 
        # Transformer Path
        # ../py/
        transformer_path =  os.path.join(root_path, self.target_pith)
        self.transformer_path = transformer_path
        
        # Template Name
        # Cypher.py
        template_file_name = transformer_class + "." + self.target_pith
        self.template_file_name = template_file_name

        # Template Path
        # ../py/Cypher.py
        template_file_path = os.path.join(transformer_path, template_file_name)
        self.template_file_path = template_file_path
        
        # Target Name
        # trees.json
        # trees.py
        target_file_name = domain_name + "." + self.target_pith # trees.json
        self.target_file_name = target_file_name
        
        # Target Path
        # ../domain/trees/json/trees.json 
        # ../domain/trees/py/trees.py         
        target_file_path = os.path.join(target_directory_path, target_file_name)
        self.target_file_path = target_file_path

        Esse.printf("Data: {data_directory_path}  Source: {source_file_path}  Output: {target_directory_path}  Target: {target_file_path}\n", BLUE, source_file_path=source_file_path, data_directory_path=data_directory_path, target_directory_path=target_directory_path, target_file_path=target_file_path)
        DF.printf(source_file_pith , df)

class Manifestation(Esse):
    def __init__(self, domain_name, resource_class, transformer_class, source_file_pith):
        Esse.printf("____________Manifestation____________\n", GREEN)

        self.name = domain_name
        self.root_path = ROOT_PATH                                      # ../
        self.domains_path = os.path.join(ROOT_PATH, DOMAIN_PITH)        # ../domain
        self.domain_path = os.path.join(self.domains_path, domain_name) # ../domain/trees
        self.resource_class = resource_class                            # Graph
        self.transformer_class = transformer_class                      # Force
        self.source_file_pith = source_file_pith                        # Exceptional_Trees
    
        # Manifestation <- ... is the umbrella (Parent) and each Child will set itself and Parent

        # Manifest
        manifest = Manifest(self)
              
        # Domain
        domain = Domain(self)
        
        # Transformer
        transformer = Esse.instantiate(transformer_class, self)
        
        # Resource
        resource = Esse.instantiate(resource_class, self)

        # Generate
        generate = Esse.extricate(resource, GENERATION_METHOD) # generate method for Graph or Platform    
        generate()
    
# Create Domain and Generate Resource (Platform or Graph) and specific type (NeoModel for Platform or Cypher for Graph;)
def main(domain_name, resource_class, transformer_class, source_file_pith):
    Esse.printf("____________Main____________\n", GREEN)         
    Esse.printf("Domain: {domain_name}  Resource: {resource_class}  Transformation: {transformer_class} Source: {source_file_pith}\n", BLUE, domain_name=domain_name, resource_class=resource_class, transformer_class=transformer_class, source_file_pith=source_file_pith)

    Manifestation(domain_name, resource_class, transformer_class, source_file_pith)

if __name__ == '__main__':
    
    Esse.printf("BEGIN...\n", RED)
    parser = argparse.ArgumentParser()
    parser.add_argument('domain_name', help="Domain")
    parser.add_argument('resource_class', help="Resource Class")
    parser.add_argument('transformer_class', help="Transformer Class")
    parser.add_argument('source_file_pith', help="Source")  
    args = parser.parse_args()
    main(args.domain_name, args.resource_class, args.transformer_class, args.source_file_pith)
    Esse.printf("\n...END", RED)

# pushd ~/radialnexus/py

# python3 manifest.py journeys Graph Force journeys
# python3 manifest.py trees Graph Force Exceptional_Trees_On_Oahu
# python3 manifest.py trees Graph Cypher Exceptional_Trees_On_Oahu


      



        
