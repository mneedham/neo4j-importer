package org.neo4j.importer;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;

import java.io.File;
import java.io.IOException;

public class Neo4jImporter {

    private Neo4jServer neo4jServer;
    private Nodes nodes;
    private RelationshipsParser relationshipsParser;

    public Neo4jImporter(Neo4jServer neo4jServer, Nodes nodes, RelationshipsParser relationshipsParser ) {
        this.neo4jServer = neo4jServer;
        this.nodes = nodes;
        this.relationshipsParser = relationshipsParser;
    }

    public void run() {
        System.out.println("Importing data into your neo4j database...");

        System.out.println("Importing nodes...");
        CreateNodesResponse createNodesResponse = neo4jServer.importNodes(nodes);

        System.out.println("Importing relationships...");
        neo4jServer.importRelationships( relationshipsParser, createNodesResponse );
    }

    public static void main( String[] args ) throws IOException {
        int batchSize = 50;
        if(args.length > 0 && args[0] != null) {
            batchSize = Integer.valueOf(args[0]);
        }

        int batchWithinBatchSize = 25;
        if(args.length > 1 && args[1] != null) {
            batchWithinBatchSize = Integer.valueOf(args[0]);
        }

        Nodes nodes = new Nodes(new File("nodes.csv"));
        RelationshipsParser relationshipsParser = new RelationshipsParser(new File("relationships.csv"));
        Neo4jServer neo4jServer = new Neo4jServer(jerseyClient(), batchSize, batchWithinBatchSize );

        new Neo4jImporter(neo4jServer, nodes, relationshipsParser ).run();
    }

    private static Client jerseyClient() {
        DefaultClientConfig defaultClientConfig = new DefaultClientConfig();
        defaultClientConfig.getClasses().add( JacksonJsonProvider.class );
        return Client.create( defaultClientConfig );
    }
}
