package org.neo4j.importer;

import com.googlecode.totallylazy.*;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.googlecode.totallylazy.Sequences.map;
import static com.googlecode.totallylazy.Sequences.sequence;
import static com.googlecode.totallylazy.numbers.Numbers.range;


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
        List<Map<String, Object>> rels = relationships.get();

        ClientResponse response = null;
        for (int i = 0; i < rels.size(); i += batchSize) {
            System.out.println( "i = " + i );
            final Sequence<Map<String, Object>> relationshipsBatch = sequence(rels).drop(i).take(batchSize);
            response = postCypherQuery(relationshipsBatch, sequence(nodeMappings.entrySet()).filter(existsIn(relationshipsBatch)));
        }

        return response;
    }

    private Predicate<Map.Entry<String, Long>> existsIn(final Sequence<Map<String, Object>> relationshipsBatch) {
        return new Predicate<Map.Entry<String, Long>>() {
            public boolean matches(Map.Entry<String, Long> idToNodeId) {
                String id = idToNodeId.getKey();
                for (Map<String, Object> fields : relationshipsBatch) {
                    if (fields.get("to").toString().equals(id) || fields.get("from").toString().equals(id)) {
                        return true;
                    }
                }
                return false;
            }
        };
    }

    private ClientResponse postCypherQuery(Sequence<Map<String, Object>> relationships, final Sequence<Map.Entry<String, Long>> nodeMappings) {
        int numberOfNodes = batchSize * 2;
        Sequence<Pair<Number,Number>> nodePairs = range(1, numberOfNodes - 1, 2).zip(range(2, numberOfNodes, 2));

        String query = "START ";
        query += StringUtils.join(nodePairs.zip(relationships).map(nodeLookup2()).iterator(), ", ");
        query += StringUtils.join(relationships.zip(nodePairs).map(createRelationship()).iterator(), " ");

        ObjectNode cypherQuery = JsonNodeFactory.instance.objectNode();
        cypherQuery.put("query", query);

        Sequence<Pair<Number, Number>> relationshipMappings = nodeParameterMappings( nodeMappings,
                nodePairs.zip( relationships ) );

        ObjectNode params = JsonNodeFactory.instance.objectNode();

        for ( Pair<Number, Number> relationshipMapping : relationshipMappings )
        {
            params.put(relationshipMapping.first().toString(), relationshipMapping.second().longValue());
        }

        cypherQuery.put("params", params);

        return client.resource(clientUri).
                accept(MediaType.APPLICATION_JSON).
                entity(cypherQuery, MediaType.APPLICATION_JSON).
                post(ClientResponse.class);
    }

    private Sequence<Pair<Number, Number>> nodeParameterMappings( final Sequence<Map.Entry<String, Long>> nodeMappings,
                                                                  Sequence<Pair<Pair<Number, Number>, Map<String,
                                                                          Object>>> relationshipMappings )
    {
        return relationshipMappings.flatMap( new Callable1<Pair<Pair<Number, Number>, Map<String, Object>>,
                Iterable<Pair<Number, Number>>>()
        {
            @Override
            public Iterable<Pair<Number, Number>> call( final Pair<Pair<Number, Number>, Map<String,
                    Object>> pairMapPair ) throws Exception
            {
                final Map<String, Object> relationship = pairMapPair.second();

                final Option<Map.Entry<String, Long>> fromId = nodeMappings.find( new Predicate<Map.Entry<String,
                        Long>>()
                {
                    public boolean matches( Map.Entry<String, Long> idToNodeIdMapping )
                    {
                        return idToNodeIdMapping.getKey().equals( relationship.get( "from" ).toString() );
                    }
                } );

                final Option<Map.Entry<String, Long>> toId = nodeMappings.find( new Predicate<Map.Entry<String, Long>>()
                {
                    public boolean matches( Map.Entry<String, Long> idToNodeIdMapping )
                    {
                        return idToNodeIdMapping.getKey().equals( relationship.get( "to" ).toString() );
                    }
                } );

                ArrayList<Pair<Number, Number>> pairs = new ArrayList<Pair<Number, Number>>();

                Number fromSequenceId = pairMapPair.first().first();
                pairs.add( Pair.<Number, Number>pair( fromSequenceId, fromId.get().getValue() ) );

                Number toSequenceId = pairMapPair.first().second();
                pairs.add( Pair.<Number, Number>pair( toSequenceId, toId.get().getValue() ) );

                return pairs;
            }
        } );
    }

    private Callable1<? super Pair<Map<String, Object>, Pair<Number, Number>>, ?> createRelationship() {
        return new Callable1<Pair<Map<String, Object>, Pair<Number, Number>>, Object>() {
            public Object call(Pair<Map<String, Object>, Pair<Number, Number>> mapPairPair) throws Exception {
                String sourceNode = "node" + mapPairPair.second().first();
                String destinationNode = "node" + mapPairPair.second().second();
                String relationshipType = mapPairPair.first().get("type").toString();
                return String.format(" CREATE %s-[:%s]->%s", sourceNode, relationshipType, destinationNode);
            }
        };
    }

    private Callable1<Pair<Pair<Number, Number>, Map<String, Object>>, Object> nodeLookup2() {
        return new Callable1<Pair<Pair<Number, Number>, Map<String, Object>>, Object>() {
            public Object call(final Pair<Pair<Number, Number>, Map<String, Object>> pairMapPair) throws Exception {
                return String.format("node%s = node({%s}), node%s=node({%s})",
                        pairMapPair.first().first(),
                        pairMapPair.first().first(),
                        pairMapPair.first().second(),
                        pairMapPair.first().second()                        );
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

}
