package org.neo4j.importer;


import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Ignore;
import org.junit.Test;

import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class Neo4jImporterTest {
    @Test
    @Ignore
    public void shouldImportTwoNodesAndARelationshipBetweenThem() {
        Client client = jerseyClient();

        Nodes nodes = mock(Nodes.class);
        Relationships relationships = mock(Relationships.class);

        ArrayNode createNodesParameters = JsonNodeFactory.instance.arrayNode();
        createNodesParameters.add(node("1", "Mark"));
        createNodesParameters.add(node("2", "Andreas"));
        when(nodes.queryParameters()).thenReturn(createNodesParameters);

        List<Map<String, Object>> relationshipsProperties = new ArrayList<Map<String, Object>>();
        relationshipsProperties.add(relationship("1", "2", "FRIEND_OF"));

        when(relationships.get()).thenReturn(relationshipsProperties);

        new Neo4jImporter(new Neo4jServer(client), nodes, relationships).run();

        String query = " START p1 = node:node_auto_index(name=\"Mark\"), p2 = node:node_auto_index(name=\"Andreas\")";
        query       += " MATCH p1-[:FRIEND_OF]->p2";
        query       += " RETURN p1.name, p2.name";

        ObjectNode cypherQuery = JsonNodeFactory.instance.objectNode();
        cypherQuery.put("query", query);
        cypherQuery.put("params", JsonNodeFactory.instance.objectNode());

        ClientResponse clientResponse = client.
                resource("http://localhost:7474/db/data/cypher").
                accept(MediaType.APPLICATION_JSON).
                entity(cypherQuery, MediaType.APPLICATION_JSON).
                post(ClientResponse.class);

        JsonNode response = clientResponse.getEntity(JsonNode.class);
        System.out.println("clientResponse.getEntity(String.class) = " + response);
        assertEquals(2, response.get("data").size());
    }

    private Map<String, Object> relationship(String from, String to, String type) {
        Map<String, Object> relationship = new HashMap<String, Object>();
        relationship.put("from", from);
        relationship.put("to", to);
        relationship.put("type", type);
        return relationship;
    }

    private ObjectNode node(String id, String name) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("id", id);
        node.put("name", name);
        return node;
    }

    private static Client jerseyClient() {
        DefaultClientConfig defaultClientConfig = new DefaultClientConfig();
        defaultClientConfig.getClasses().add( JacksonJsonProvider.class );
        return Client.create( defaultClientConfig );
    }

}
