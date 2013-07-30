package org.neo4j.importer;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;

import java.io.File;
import java.io.IOException;

public class Neo4jImporter {
    public static void main(String[] args) throws IOException {
        System.out.println("Importing data into your neo4j database...");

        Nodes nodes = new Nodes(new File("nodes.csv"));
        Relationships relationships = new Relationships(new File("relationships.csv"));

        Neo4jServer neo4jServer = new Neo4jServer(jerseyClient());

        System.out.println("Importing nodes: " + neo4jServer.importNodes(nodes).getStatus());
        System.out.println("Importing relationships: " + neo4jServer.importRelationships(relationships).getStatus());

    }

    private static Client jerseyClient() {
        DefaultClientConfig defaultClientConfig = new DefaultClientConfig();
        defaultClientConfig.getClasses().add( JacksonJsonProvider.class );
        return Client.create( defaultClientConfig );
    }
}
