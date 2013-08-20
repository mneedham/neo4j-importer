package org.neo4j.importer;

import com.googlecode.totallylazy.Callable1;
import com.googlecode.totallylazy.Sequences;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.Map;

import static com.googlecode.totallylazy.Sequences.map;
import static org.apache.commons.lang.StringUtils.join;


public class Neo4jServer {
    private Client client;
    private String clientUri = "http://localhost:7474/db/data/cypher";

    public Neo4jServer(Client client) {
        this.client = client;
    }

    public CreateNodesResponse importNodes(Nodes nodes) {
        ObjectNode cypherQuery = JsonNodeFactory.instance.objectNode();

        cypherQuery.put("query", "CREATE (node {properties}) RETURN node.id, ID(node) AS nodeId");

        ObjectNode properties = JsonNodeFactory.instance.objectNode();
        properties.put("properties", nodes.queryParameters());
        cypherQuery.put("params", properties);

        ClientResponse clientResponse = client.resource(clientUri).
                accept(MediaType.APPLICATION_JSON).
                entity(cypherQuery, MediaType.APPLICATION_JSON).
                post(ClientResponse.class);
        return new CreateNodesResponse(clientResponse.getStatus(), clientResponse.getEntity(JsonNode.class));
    }

    public ClientResponse importRelationships(Relationships relationships, CreateNodesResponse createNodesResponse) {
        Map<String, Long> nodeMappings = createNodesResponse.nodeMappings();

        List<Map<String, Object>> actualRelationships = relationships.get();

        String query = "START ";
        query += join(map(nodeMappings.entrySet(), nodeLookup()).iterator(), ", ");
        query += join(map(actualRelationships, createRelationship()).iterator(), " ");

        System.out.println("query = " + query);

        ObjectNode cypherQuery = JsonNodeFactory.instance.objectNode();
        cypherQuery.put("query", query);

        ObjectNode params = JsonNodeFactory.instance.objectNode();
        params.put("1", 1);
        params.put("2", 2);
        params.put("3", 3);

        cypherQuery.put("params", params);

        return client.resource(clientUri).
                accept(MediaType.APPLICATION_JSON).
                entity(cypherQuery, MediaType.APPLICATION_JSON).
                post(ClientResponse.class);
    }

    private Callable1<? super Map<String, Object>, ?> createRelationship() {
        return new Callable1<Map<String, Object>, Object>() {
            public Object call(Map<String, Object> stringObjectMap) throws Exception {
                String sourceNode = "node" + stringObjectMap.get("from").toString();
                String destinationNode = "node" + stringObjectMap.get("to").toString();
                String relationshipType = stringObjectMap.get("type").toString();
                return addRelationship(sourceNode, destinationNode, relationshipType);
            }
        };
    }

    private Callable1<? super Map.Entry<String, Long>, ?> nodeLookup() {
        return new Callable1<Map.Entry<String, Long>, String>() {
            public String call(Map.Entry<String, Long> stringLongEntry) throws Exception {
                return String.format("node%s = node(%s)", stringLongEntry.getKey(), stringLongEntry.getValue().toString());
            }
        };
    }

    private String addRelationship(String source, String destination, String relationshipType) {
        return  " CREATE " + source + "-[:" + relationshipType + "]->" + destination;
    }
}
