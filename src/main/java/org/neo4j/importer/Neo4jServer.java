package org.neo4j.importer;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import javax.ws.rs.core.MediaType;


public class Neo4jServer {
    private Client client;

    public Neo4jServer(Client client) {
        this.client = client;
    }

    public ClientResponse importData(Nodes nodes) {
        ObjectNode cypherQuery = JsonNodeFactory.instance.objectNode();

        cypherQuery.put("query", "CREATE ({properties})");

        ObjectNode properties = JsonNodeFactory.instance.objectNode();
        properties.put("properties", nodes.queryParameters());
        cypherQuery.put("params", properties);

        return client.resource("http://localhost:7474/db/data/cypher").
                accept(MediaType.APPLICATION_JSON).
                entity(cypherQuery, MediaType.APPLICATION_JSON).
                post(ClientResponse.class);
    }
}
