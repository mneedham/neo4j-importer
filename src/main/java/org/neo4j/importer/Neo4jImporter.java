package org.neo4j.importer;

import au.com.bytecode.opencsv.CSVReader;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class Neo4jImporter {
    public static void main(String[] args) throws IOException {
        System.out.println("Importing data into your neo4j database...");

        Nodes nodes = new Nodes(new File("nodes.csv"));

        DefaultClientConfig defaultClientConfig = new DefaultClientConfig();
        defaultClientConfig.getClasses().add( JacksonJsonProvider.class );
        Client client = Client.create( defaultClientConfig );

        Neo4jServer neo4jServer = new Neo4jServer(client);

        ClientResponse clientResponse = neo4jServer.importData(nodes);

        System.out.println("clientResponse.getStatus() = " + clientResponse.getStatus());
        System.out.println("clientResponse.getStatus() = " + clientResponse.getEntity(String.class));
    }
}
