package org.neo4j.importer;

import com.googlecode.totallylazy.*;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.googlecode.totallylazy.Sequences.map;
import static com.googlecode.totallylazy.Sequences.sequence;
import static com.googlecode.totallylazy.numbers.Numbers.range;


public class Neo4jServer
{
    private Client client;
    private String clientUri = "http://localhost:7474/db/data/cypher";
    private String transactionalUri = "http://localhost:7474/db/data/transaction/commit";
    private int batchSize;

    public Neo4jServer( Client client, int batchSize )
    {
        this.batchSize = batchSize;
        this.client = client;
    }

    public CreateNodesResponse importNodes( Nodes nodes )
    {
        ObjectNode cypherQuery = JsonNodeFactory.instance.objectNode();
        cypherQuery.put( "query", "CREATE (node {properties}) RETURN node.id, ID(node) AS nodeId" );

        ObjectNode properties = JsonNodeFactory.instance.objectNode();
        properties.put( "properties", nodes.queryParameters() );
        cypherQuery.put( "params", properties );

        ClientResponse clientResponse = client.resource( clientUri ).
                accept( MediaType.APPLICATION_JSON ).
                entity( cypherQuery, MediaType.APPLICATION_JSON ).
                post( ClientResponse.class );
        return new CreateNodesResponse( clientResponse.getStatus(), clientResponse.getEntity( JsonNode.class ) );
    }

    public ClientResponse importRelationships( Relationships relationships, CreateNodesResponse createNodesResponse )
    {
        Map<String, Long> nodeMappings = createNodesResponse.nodeMappings();
        Sequence<Map<String, Object>> rels = sequence( relationships.get() ).take(1000);
        int numberOfRelationshipsToImport = rels.size();


        ObjectNode query = JsonNodeFactory.instance.objectNode();
        ArrayNode statements = JsonNodeFactory.instance.arrayNode();

        for ( int i = 0; i < numberOfRelationshipsToImport; i += batchSize )
        {
            final Sequence<Map<String, Object>> relationshipsBatch = rels.drop( i ).take( batchSize );
            ObjectNode statement = createStatement( relationshipsBatch, nodeMappings );
            statements.add( statement );
        }

        query.put( "statements", statements );

        ClientResponse transactionResponse = client.resource( transactionalUri ).
                accept( MediaType.APPLICATION_JSON ).
                entity( query, MediaType.APPLICATION_JSON ).
                post( ClientResponse.class );

        return transactionResponse;
    }

    private Predicate<Map.Entry<String, Long>> existsIn( final Sequence<Map<String, Object>> relationshipsBatch )
    {
        return new Predicate<Map.Entry<String, Long>>()
        {
            public boolean matches( Map.Entry<String, Long> idToNodeId )
            {
                String id = idToNodeId.getKey();
                for ( Map<String, Object> fields : relationshipsBatch )
                {
                    if ( fields.get( "to" ).toString().equals( id ) || fields.get( "from" ).toString().equals( id ) )
                    {
                        return true;
                    }
                }
                return false;
            }
        };
    }

    private ObjectNode createStatement( Sequence<Map<String, Object>> relationships, Map<String, Long> mappings )
    {
        int numberOfNodes = batchSize * 2;
        Sequence<Pair<Number, Number>> nodePairs = range( 1, numberOfNodes - 1, 2 ).zip( range( 2, numberOfNodes, 2 ) );

        String query = "START ";
        query += StringUtils.join( nodePairs.zip( relationships ).map( nodeLookup2() ).iterator(), ", " );
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


//        return relationshipMappings.flatMap( new Callable1<Pair<Pair<Number, Number>, Map<String, Object>>,
//                Iterable<Pair<Number, Number>>>()
//        {
//            @Override
//            public Iterable<Pair<Number, Number>> call( final Pair<Pair<Number, Number>, Map<String,
//                    Object>> pairMapPair ) throws Exception
//            {
//                final Map<String, Object> relationship = pairMapPair.second();
//
//                final Option<Map.Entry<String, Long>> fromId = nodeMappings.find( new Predicate<Map.Entry<String,
//                        Long>>()
//                {
//                    public boolean matches( Map.Entry<String, Long> idToNodeIdMapping )
//                    {
//                        return idToNodeIdMapping.getKey().equals( relationship.get( "from" ).toString() );
//                    }
//                } );
//
//                final Option<Map.Entry<String, Long>> toId = nodeMappings.find( new Predicate<Map.Entry<String,
// Long>>()
//                {
//                    public boolean matches( Map.Entry<String, Long> idToNodeIdMapping )
//                    {
//                        return idToNodeIdMapping.getKey().equals( relationship.get( "to" ).toString() );
//                    }
//                } );
//
//                ArrayList<Pair<Number, Number>> pairs = new ArrayList<Pair<Number, Number>>();
//
//                Number fromSequenceId = pairMapPair.first().first();
//                pairs.add( Pair.<Number, Number>pair( fromSequenceId, fromId.get().getValue() ) );
//
//                Number toSequenceId = pairMapPair.first().second();
//                pairs.add( Pair.<Number, Number>pair( toSequenceId, toId.get().getValue() ) );
//
//                return pairs;
//            }
//        } );


    private Callable1<? super Pair<Map<String, Object>, Pair<Number, Number>>, ?> createRelationship()
    {
        return new Callable1<Pair<Map<String, Object>, Pair<Number, Number>>, Object>()
        {
            public Object call( Pair<Map<String, Object>, Pair<Number, Number>> mapPairPair ) throws Exception
            {
                String sourceNode = "node" + mapPairPair.second().first();
                String destinationNode = "node" + mapPairPair.second().second();
                String relationshipType = mapPairPair.first().get( "type" ).toString();
                return String.format( " CREATE %s-[:%s]->%s", sourceNode, relationshipType, destinationNode );
            }
        };
    }

    private Callable1<Pair<Pair<Number, Number>, Map<String, Object>>, Object> nodeLookup2()
    {
        return new Callable1<Pair<Pair<Number, Number>, Map<String, Object>>, Object>()
        {
            public Object call( final Pair<Pair<Number, Number>, Map<String, Object>> pairMapPair ) throws Exception
            {
                return String.format( "node%s = node({%s}), node%s=node({%s})",
                        pairMapPair.first().first(),
                        pairMapPair.first().first(),
                        pairMapPair.first().second(),
                        pairMapPair.first().second() );
            }
        };
    }

    private Callable1<? super Map.Entry<String, Long>, ?> nodeLookup()
    {
        return new Callable1<Map.Entry<String, Long>, String>()
        {
            public String call( Map.Entry<String, Long> mapping ) throws Exception
            {
                return String.format( "node%s = node(%s)", mapping.getKey(), mapping.getValue().toString() );
            }
        };
    }

}
