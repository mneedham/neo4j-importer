package org.neo4j.importer;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;

import java.io.File;
import java.io.IOException;

public class Neo4jImporter {

    private Neo4jServer neo4jServer;
    private Nodes nodes;
    private Relationships relationships;

    public Neo4jImporter(Neo4jServer neo4jServer, Nodes nodes, Relationships relationships) {
        this.neo4jServer = neo4jServer;
        this.nodes = nodes;
        this.relationships = relationships;
    }

    public void run() {
        System.out.println("Importing data into your neo4j database...");

        CreateNodesResponse createNodesResponse = neo4jServer.importNodes(nodes);
        System.out.println("Importing nodes: " + createNodesResponse.getStatus());
        System.out.println("Importing nodes: " + createNodesResponse.getOutput());

        ClientResponse createRelationshipsResponse = neo4jServer.importRelationships(relationships, createNodesResponse);
        System.out.println("Importing relationships: " + createRelationshipsResponse.getStatus());
        System.out.println("createRelationshipsResponse = " + createRelationshipsResponse.getEntity(String.class));
    }



    public static void main(String[] args) throws IOException {
        Nodes nodes = new Nodes(new File("nodes.csv"));
        Relationships relationships = new Relationships(new File("relationships.csv"));
        Neo4jServer neo4jServer = new Neo4jServer(jerseyClient(), 50);

        new Neo4jImporter(neo4jServer, nodes, relationships).run();
    }

    private static Client jerseyClient() {
        DefaultClientConfig defaultClientConfig = new DefaultClientConfig();
        defaultClientConfig.getClasses().add( JacksonJsonProvider.class );
        return Client.create( defaultClientConfig );
    }
}
