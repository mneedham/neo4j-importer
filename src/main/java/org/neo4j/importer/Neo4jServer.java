package org.neo4j.importer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MediaType;

import com.googlecode.totallylazy.Callable1;
import com.googlecode.totallylazy.Pair;
import com.googlecode.totallylazy.Sequence;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import static com.googlecode.totallylazy.Sequences.sequence;
import static com.googlecode.totallylazy.numbers.Numbers.range;


public class Neo4jServer
{
    private static final String NODE_LOOKUP = "node%s = node({%s}), node%s=node({%s})";
    private static final String CREATE_RELATIONSHIP = " CREATE %s-[:%s]->%s";
    private static final String CREATE_NODE = "CREATE (node {properties}) RETURN node.id, ID(node) AS nodeId";

    private Client client;
    private String clientUri = "http://localhost:7474/db/data/cypher";
    private String transactionalUri = "http://localhost:7474/db/data/transaction/commit";
    private int batchSize;
    private int batchWithinBatchSize;

    public Neo4jServer( Client client, int batchSize, int batchWithinBatchSize )
    {
        this.batchSize = batchSize;
        this.batchWithinBatchSize = batchWithinBatchSize;
        this.client = client;
    }

    public CreateNodesResponse importNodes( Nodes nodes )
    {
        ObjectNode cypherQuery = JsonNodeFactory.instance.objectNode();
        cypherQuery.put( "query", CREATE_NODE );

        ObjectNode properties = JsonNodeFactory.instance.objectNode();
        properties.put( "properties", nodes.queryParameters() );
        cypherQuery.put( "params", properties );

        ClientResponse clientResponse = client.resource( clientUri ).
                accept( MediaType.APPLICATION_JSON ).
                entity( cypherQuery, MediaType.APPLICATION_JSON ).
                post( ClientResponse.class );
        return new CreateNodesResponse( clientResponse.getStatus(), clientResponse.getEntity( JsonNode.class ) );
    }

    public ClientResponse importRelationships( RelationshipsParser relationshipsParser, CreateNodesResponse createNodesResponse )
    {
        Map<String, Long> nodeMappings = createNodesResponse.nodeMappings();
        Sequence<Map<String, Object>> rels = sequence( relationshipsParser.relationships() );

        int numberOfRelationshipsToImport = rels.size();

        ClientResponse transactionResponse = null;
        for ( int i = 0; i < numberOfRelationshipsToImport; i += batchSize ) {
            Sequence<Map<String, Object>> batchRels = rels.drop( i ).take( batchSize );

            long beforeBuildingQuery = System.currentTimeMillis();
            ObjectNode query = JsonNodeFactory.instance.objectNode();
            ArrayNode statements = JsonNodeFactory.instance.arrayNode();
            for ( int j = 0; j < batchSize; j += batchWithinBatchSize )
            {
                {
                    final Sequence<Map<String, Object>> relationshipsBatch = batchRels.drop( j ).take( batchWithinBatchSize );
                    ObjectNode statement = createStatement( relationshipsBatch, nodeMappings );
                    statements.add( statement );
                }
            }

            query.put( "statements", statements );
            System.out.println( "building query: " + (System.currentTimeMillis() - beforeBuildingQuery ));

            long beforePosting = System.currentTimeMillis();
            transactionResponse = client.resource( transactionalUri ).
                    accept( MediaType.APPLICATION_JSON ).
                    entity( query, MediaType.APPLICATION_JSON ).
                    header( "X-Stream", true ).
                    post( ClientResponse.class );
            System.out.println( "to neo and back: " + (System.currentTimeMillis() - beforePosting ));
        }



        return transactionResponse;
    }

    private ObjectNode createStatement( Sequence<Map<String, Object>> relationships, Map<String, Long> mappings )
    {
        int numberOfNodes = batchSize * 2;
        Sequence<Pair<Number, Number>> nodePairs = range( 1, numberOfNodes - 1, 2 ).zip( range( 2, numberOfNodes, 2 ) );

        String query = "START ";
        query += StringUtils.join( nodePairs.zip( relationships ).map( nodeLookup() ).iterator(), ", " );
        query += StringUtils.join( relationships.zip( nodePairs ).map( createRelationship() ).iterator(), " " );

        ObjectNode cypherQuery = JsonNodeFactory.instance.objectNode();
        cypherQuery.put( "statement", query );

        List<Pair<Number, Number>> relationshipMappings = nodeParameterMappings(
                nodePairs.zip( relationships ), mappings );

        ObjectNode params = JsonNodeFactory.instance.objectNode();

        for ( Pair<Number, Number> relationshipMapping : relationshipMappings )
        {
            params.put( relationshipMapping.first().toString(), relationshipMapping.second().longValue() );
        }

        cypherQuery.put( "parameters", params );

        return cypherQuery;
    }

    private List<Pair<Number, Number>> nodeParameterMappings( final Sequence<Pair<Pair<Number, Number>, Map<String,
            Object>>> relationshipMappings, Map<String,
            Long> mappings )
    {
        List<Pair<Number, Number>> pairs = new ArrayList<Pair<Number, Number>>();
        for ( Pair<Pair<Number, Number>, Map<String, Object>> pairMapPair : relationshipMappings )
        {
            final Map<String, Object> relationship = pairMapPair.second();

            Long from = mappings.get( relationship.get( "from" ).toString() );
            Long to = mappings.get( relationship.get( "to" ).toString() );

            Number fromSequenceId = pairMapPair.first().first();
            pairs.add( Pair.<Number, Number>pair( fromSequenceId, from ) );

            Number toSequenceId = pairMapPair.first().second();
            pairs.add( Pair.<Number, Number>pair( toSequenceId, to ) );
        }
        return pairs;
    }

    private Callable1<? super Pair<Map<String, Object>, Pair<Number, Number>>, ?> createRelationship()
    {
        return new Callable1<Pair<Map<String, Object>, Pair<Number, Number>>, Object>()
        {
            public Object call( Pair<Map<String, Object>, Pair<Number, Number>> mapPairPair ) throws Exception
            {
                String sourceNode = "node" + mapPairPair.second().first();
                String destinationNode = "node" + mapPairPair.second().second();
                String relationshipType = mapPairPair.first().get( "type" ).toString();
                return String.format( CREATE_RELATIONSHIP, sourceNode, relationshipType, destinationNode );
            }
        };
    }

    private Callable1<Pair<Pair<Number, Number>, Map<String, Object>>, Object> nodeLookup()
    {
        return new Callable1<Pair<Pair<Number, Number>, Map<String, Object>>, Object>()
        {
            public Object call( final Pair<Pair<Number, Number>, Map<String, Object>> pairMapPair ) throws Exception
            {
                return String.format( NODE_LOOKUP,
                        pairMapPair.first().first(),
                        pairMapPair.first().first(),
                        pairMapPair.first().second(),
                        pairMapPair.first().second() );
            }
        };
    }

}
