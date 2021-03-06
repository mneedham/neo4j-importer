package org.neo4j.importer;

import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RelationshipsParserTest
{
    @Test
    public void shouldCreateCollectionOfRelationships() {
        RelationshipsParser relationshipsParser = new RelationshipsParser(new File("src/resources/relationships.csv"));

        List<Map<String,Object>> expectedRelationships = new ArrayList<Map<String, Object>>();

        HashMap<String, Object> relationship = new HashMap<String, Object>();
        relationship.put("from", "1");
        relationship.put("to", "2");
        relationship.put("type", "FRIEND");
        relationship.put("timeInMonths", "3");

        expectedRelationships.add(relationship);

        assertEquals(expectedRelationships, relationshipsParser.relationships());
    }
}
