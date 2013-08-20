package org.neo4j.importer;

import com.googlecode.totallylazy.Callable1;
import com.googlecode.totallylazy.Sequence;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import javax.ws.rs.core.MediaType;
import java.util.Iterator;
import java.util.Map;

import static com.googlecode.totallylazy.Sequences.map;
import static com.googlecode.totallylazy.Sequences.sequence;
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
        int batchSize = 50;
        Map<String, Long> nodeMappings = createNodesResponse.nodeMappings();

        String query = "START ";
        query += join(map(nodeMappings.entrySet(), nodeLookup()).iterator(), ", ");

        Sequence<Object> rels2 = map(sequence(relationships.get()), createRelationship());

        ClientResponse response = null;
        for (int i = 0; i < rels2.size(); i+= batchSize) {
            response = post(query, rels2.drop(i).take(batchSize).iterator());
//            System.out.println("response = " + response);
        }

        return response;
    }

    private ClientResponse post(String query, Iterator<Object> relationships) {
        query += join(relationships, " ");

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
            public Object call(Map<String, Object> fields) throws Exception {
                String sourceNode = "node" + fields.get("from").toString();
                String destinationNode = "node" + fields.get("to").toString();
                String relationshipType = fields.get("type").toString();
                return addRelationship(sourceNode, destinationNode, relationshipType);
            }
        };
    }

    private Callable1<? super Map.Entry<String, Long>, ?> nodeLookup() {
        return new Callable1<Map.Entry<String, Long>, String>() {
            public String call(Map.Entry<String, Long> mapping) throws Exception {
                return String.format("node%s = node(%s)", mapping.getKey(), mapping.getValue().toString());
            }
        };
    }

    private String addRelationship(String source, String destination, String relationshipType) {
        return  " CREATE " + source + "-[:" + relationshipType + "]->" + destination;
    }
}
