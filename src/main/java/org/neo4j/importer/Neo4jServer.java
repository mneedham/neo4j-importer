package org.neo4j.importer;

import com.googlecode.totallylazy.Callable1;
import com.googlecode.totallylazy.Predicate;
import com.googlecode.totallylazy.Sequence;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import javax.ws.rs.core.MediaType;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.googlecode.totallylazy.Sequences.filter;
import static com.googlecode.totallylazy.Sequences.map;
import static com.googlecode.totallylazy.Sequences.sequence;
import static org.apache.commons.lang.StringUtils.join;


public class Neo4jServer {
    private Client client;
    private String clientUri = "http://localhost:7474/db/data/cypher";
    private int batchSize;

    public Neo4jServer(Client client, int batchSize) {
        this.batchSize = batchSize;
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
        List<Map<String,Object>> rels = relationships.get();

        ClientResponse response = null;
        for (int i = 0; i < rels.size(); i+= batchSize) {
            final Sequence<Map<String, Object>> relsToImport = sequence(rels).drop(i).take(batchSize);
            Sequence<Object> cypherRelationshipStatements = map(relsToImport, createRelationship());

            Sequence<Map.Entry<String, Long>> filteredMappings = filter(nodeMappings.entrySet(), new Predicate<Map.Entry<String, Long>>() {
                public boolean matches(Map.Entry<String, Long> idToNodeId) {
                    String id = idToNodeId.getKey();
                    for (Map<String, Object> fields : relsToImport) {
                        if (fields.get("to").toString().equals(id) || fields.get("from").toString().equals(id)) {
                            return true;
                        }
                    }
                    return false;
                }
            });


            response = postCypherQuery(cypherRelationshipStatements.iterator(), filteredMappings);
        }

        return response;
    }

    private ClientResponse postCypherQuery(Iterator<Object> relationships, Sequence<Map.Entry<String,Long>> nodeMappings) {
        String query = "START ";
        query += join(map(nodeMappings, nodeLookup()).iterator(), ", ");
        query += join(relationships, " ");

        ObjectNode cypherQuery = JsonNodeFactory.instance.objectNode();
        cypherQuery.put("query", query);

        ObjectNode params = JsonNodeFactory.instance.objectNode();
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
