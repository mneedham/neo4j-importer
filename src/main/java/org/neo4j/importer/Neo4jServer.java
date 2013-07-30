package org.neo4j.importer;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.Map;


public class Neo4jServer {
    private Client client;
    private String clientUri = "http://localhost:7474/db/data/cypher";

    public Neo4jServer(Client client) {
        this.client = client;
    }

    public ClientResponse importNodes(Nodes nodes) {
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

    public ClientResponse importRelationships(Relationships relationships) {
        String query = "";
        query += "START node1=node(1), node2=node(2)\n";
        query += "CREATE UNIQUE node1-[:FRIEND_OF]->node2";

//        for (Map<String, Object> relationshipsProperty : relationships.get()) {
//            query += "START node1=node(1), node2=node(2)\n";
//            query += "CREATE UNIQUE node1-[:FRIEND_OF]->node2";
//        }

        ObjectNode cypherQuery = JsonNodeFactory.instance.objectNode();
        cypherQuery.put("query", query);
        cypherQuery.put("params", JsonNodeFactory.instance.objectNode());

        return client.resource(clientUri).
                accept(MediaType.APPLICATION_JSON).
                entity(cypherQuery, MediaType.APPLICATION_JSON).
                post(ClientResponse.class);
    }
}
