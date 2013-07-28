repositories.remote << 'http://repo1.maven.org/maven2'

define 'neo4j-importer' do
  project.version = '0.1.0'
  package(:jar).with :manifest=>manifest.merge('Main-Class'=>'org.neo4j.importer.Neo4jImporter')
end