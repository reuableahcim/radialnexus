import os
import shutil
import argparse
import yaml

def create_directory_structure(domain_name):
    """
    Create the necessary directory structure for a new domain.
    """
    base_path = os.path.join('..', 'domain', domain_name)
    directories = [
        'csv/data',
        'cypher',
        'json',
        'png',
        'yaml'
    ]
    
    for directory in directories:
        dir_path = os.path.join(base_path, directory)
        os.makedirs(dir_path, exist_ok=True)
        print(f"Created directory: {dir_path}")
    
def copy_starter_files(domain_name):
    """
    Copy starter files from the ~/sor/ directory into the new domain directories.
    """
    # Copy 00_Mapping.csv to the csv/ directory
    shutil.copy(os.path.join('..', 'sor', '00_Mapping.csv'), os.path.join('..', 'domain', domain_name, 'csv', '00_Mapping.csv'))
    print(f"Copied 00_Mapping.csv to {os.path.join('..', 'domain', domain_name, 'csv')}")

    # Copy 00_Model.yaml to the yaml/ directory
    shutil.copy(os.path.join('..', 'sor', '00_Model.yaml'), os.path.join('..', 'domain', domain_name, 'yaml', '00_Model.yaml'))
    print(f"Copied 00_Model.yaml to {os.path.join('..', 'domain', domain_name, 'yaml')}")

def modify_vue_files(domain_name, domain_yaml):
    """
    Modify Vue files to add the new domain.
    """
    # Paths to important Vue files
    vue_path = os.path.join('..', 'vue', 'src')
    main_js_path = os.path.join(vue_path, 'main.js')
    landing_page_path = os.path.join(vue_path, 'views', 'LandingPage.vue')
    domain_graph_path = os.path.join(vue_path, 'views', f'{domain_name.capitalize()}Graph.vue')
    
    # Read and modify ~/vue/src/main.js
    with open(main_js_path, 'r') as file:
        main_js = file.read()
    
    # Insert component and route into main.js
    component_insert = f"\n// Import {domain_name.capitalize()}Graph.vue component\nimport {domain_name.capitalize()}Graph from './views/{domain_name.capitalize()}Graph.vue';\n"
    route_insert = f"\n    {{ path: '/{domain_name}', component: {domain_name.capitalize()}Graph }},\n"

    if component_insert not in main_js:
        main_js = main_js.replace('// BEGIN insert component', f'// BEGIN insert component{component_insert}')
        main_js = main_js.replace('// BEGIN Insert route', f'// BEGIN Insert route{route_insert}')
    
    # Write back to main.js
    with open(main_js_path, 'w') as file:
        file.write(main_js)
    
    print(f"Modified main.js with {domain_name.capitalize()}Graph component and route")

    # Modify ~/vue/src/views/LandingPage.vue
    with open(landing_page_path, 'r') as file:
        landing_page = file.read()
    
    # Extract values from the domain yaml
    title = domain_yaml.get('title', domain_name.capitalize())
    headline = domain_yaml.get('headline', f'{domain_name.capitalize()} Headline')
    description = domain_yaml.get('description', f'{domain_name.capitalize()} description.')

    landing_page_insert = f"""
          title: '{title}',
          headline: '{headline}',			
          description: '{description}.',
          imageSrc: require('@/assets/{domain_name}.png'),
          route: '/{domain_name}'
        },"""

    if landing_page_insert not in landing_page:
        landing_page = landing_page.replace('<!-- BEGIN insert domain -->', f'<!-- BEGIN insert domain -->{landing_page_insert}')
    
    # Write back to LandingPage.vue
    with open(landing_page_path, 'w') as file:
        file.write(landing_page)

    print(f"Modified LandingPage.vue with new domain {domain_name}")

    # Copy DomainGraph.vue template and modify it
    shutil.copy(os.path.join('..', 'sor', 'DomainGraph.vue'), domain_graph_path)
    
    with open(domain_graph_path, 'r') as file:
        domain_graph_vue = file.read()

    # Modify template, name, and fetch for the new domain
    domain_graph_vue = domain_graph_vue.replace('domain-graph', f'{domain_name}-graph')
    domain_graph_vue = domain_graph_vue.replace('DomainGraph', f'{domain_name.capitalize()}Graph')
    domain_graph_vue = domain_graph_vue.replace("fetch('domain/domain.json')", f"fetch('domain/{domain_name}.json')")

    # Write back to <Domain>Graph.vue
    with open(domain_graph_path, 'w') as file:
        file.write(domain_graph_vue)
    
    print(f"Copied and modified {domain_name.capitalize()}Graph.vue")

def copy_domain_image(domain_name):
    """
    Copy the domain image from ~/sor/ to the Vue assets directory.
    """
    shutil.copy(os.path.join('..', 'sor', 'domain.png'), os.path.join('..', 'vue', 'src', 'assets', f'{domain_name}.png'))
    print(f"Copied domain.png to assets/{domain_name}.png")

def load_domain_yaml(domain_name):
    """
    Load the domain.yaml file if it exists.
    """
    yaml_path = os.path.join('..', 'yaml', f'{domain_name}.yaml')
    
    if os.path.exists(yaml_path):
        with open(yaml_path, 'r') as file:
            return yaml.safe_load(file)
    else:
        print(f"No domain.yaml file found for {domain_name}. Proceeding without it.")
        return {}

def main(domain_name, resource_type):
    """
    Main function to instantiate a new domain's directory structure and update files for Force graph visualization.
    """
    if resource_type != 'Force':
        print(f"Resource type {resource_type} not supported yet.")
        return
    
    # Step 1: Create directory structure
    create_directory_structure(domain_name)
    
    # Step 2: Copy starter files (00_Mapping.csv, 00_Model.yaml)
    copy_starter_files(domain_name)

    # Step 3: Modify Vue files
    domain_yaml = load_domain_yaml(domain_name)
    modify_vue_files(domain_name, domain_yaml)
    
    # Step 4: Copy domain.png to assets
    copy_domain_image(domain_name)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Instantiate a new domain.")
    parser.add_argument('domain_name', type=str, help='The name of the domain to instantiate.')
    parser.add_argument('resource_type', type=str, help='The type of resource (e.g., Force) to generate.')
    
    args = parser.parse_args()
    main(args.domain_name, args.resource_type)
