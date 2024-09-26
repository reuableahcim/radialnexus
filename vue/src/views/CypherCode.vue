<template>
  <div class="cypher-code">
    <pre><code>

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
		SET location.latitude = row.latitude
		SET location.longitude = row.longitude

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

	</code></pre>
  </div>
</template>

<script>
export default {
  name: 'CypherCode'
};
</script>

<style scoped>
/* Add styling as needed */
</style>