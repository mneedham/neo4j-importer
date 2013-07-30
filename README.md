*This is a work in progress, it doesn't do everything it says below yet*

# An easy to use tool for importing data into neo4j

neo4j importer is a command line tool which makes it easy to import data in a variety of different formats into your database. It will eventually support CSV, [GraphML](http://graphml.graphdrawing.org/) and [Geoff](http://nigelsmall.com/geoff) but we're open to suggestions for other formats that you'd like to see.

## Installation

1. Download the neo4j-importer zip file.

````
curl http://dist.neo4j.org/neo4j-importer.zip -o neo4j-importer.zip
````

2. Unzip it somewhere on your machine.

````
unzip batch-importer.zip -d ~/neo4j-importer
````

3. That's it!

## Usage

1. Ensure you have a neo4j server running 

    ~~~ sh
    /path/to/neo4j-installation/bin/neo4j start
    ~~~

2. Navigate on your machine to the folder where you unzipped neo4j-importer.

    ~~~ sh
    cd ~/neo4j-importer
    ~~~ 

3. Create a file names [nodes.csv](examples/nodes.csv) which contains the nodes that you want to import. 
The file should contain a header listing the node's fields. One of those fields *must* be called 'id' and is used to identify the node.

e.g.
    
    ~~~ sh
    echo -e "id\tname\n1\tMark\n2\tAndreas" > nodes.csv
    ~~~

    ~~~ sh
    cat nodes.csv
    id	name
    1	Mark
    2	Andreas
    ~~~

4. Create a file named [relationships.csv](examples/relationships.csv) which contains relationships between the nodes defined in nodes.csv. 
The file should contain a header with the fields 'from', 'to', 'type' and can also contain fields for properties on the relationship. The 'from' and 'to' fields use the values defined in the 'id' field in nodes.csv.

e.g.

````
echo -e "from\tto\ttype\ttimeInMonths\n1\t2\tFRIEND\t3" > relationships.csv
````

````
cat relationships.csv
from	to	type	timeInMonths
1	2	FRIEND	3
````

5. Import your data into neo4j

````
./bin/neo4j-importer
Successfully imported 1 node and 1 relationship
````