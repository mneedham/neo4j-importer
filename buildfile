repositories.remote << 'http://repo1.maven.org/maven2'

dependencies = []
dependencies << 'net.sf.opencsv:opencsv:jar:2.0'
dependencies << 'com.sun.jersey:jersey-bundle:jar:1.17.1'
dependencies << 'org.codehaus.jackson:jackson-core-asl:jar:1.9.4'
dependencies << 'org.codehaus.jackson:jackson-mapper-asl:jar:1.9.4'
dependencies << 'org.codehaus.jackson:jackson-jaxrs:jar:1.9.4'

define 'neo4j-importer' do
  project.version = '0.1.0'
  compile.with dependencies
  test.with 'junit:junit:jar:4.11'
  package(:jar)

	task :uberjar => :package do |t|
  
    assembly_dir = "target/assembly"
      
    FileUtils.mkdir_p( "#{assembly_dir}/lib" )
    FileUtils.mkdir_p( "#{assembly_dir}/main" )
   
    FileUtils.copy "target/#{project.name}-#{project.version}.jar", "#{assembly_dir}/main"
   
    artifacts = Buildr.artifacts(dependencies).map(&:to_s)
    artifacts.each do |artifact|
        FileUtils.copy artifact, "#{assembly_dir}/lib" 
    end
    
    Unzip.new( assembly_dir => "lib/one-jar-boot-0.97.jar" ).extract
    FileUtils.rm_rf "#{assembly_dir}/src" 
    
    File.open( "#{assembly_dir}/boot-manifest.mf", 'a') do |f| 
        f.write("One-Jar-Main-Class: org.neo4j.importer.Neo4jImporter\n")
    end
    
    `cd target/assembly; jar -cvfm ../#{project.name}-uberjar-#{project.version}.jar boot-manifest.mf .`
  end  
end