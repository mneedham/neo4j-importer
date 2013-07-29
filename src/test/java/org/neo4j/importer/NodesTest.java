package org.neo4j.importer;


import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class NodesTest {
    @Test
    public void shouldCreateCypherQueryParametersFromFile() {
        Nodes nodes = new Nodes(new File("src/resources/nodes.csv"));

        ArrayNode expectedParameters = JsonNodeFactory.instance.arrayNode();
        expectedParameters.add(createNode("1", "Mark"));
        expectedParameters.add(createNode("2", "Andreas"));

        ArrayNode actualParameters = nodes.queryParameters();

        assertEquals(expectedParameters, actualParameters);
    }

    @Test
    public void shouldBlowUpIfIdNotProvidedInHeaderLine() {
        Nodes nodes = new Nodes(new File("src/resources/nodes_no_header.csv"));

        try {
            nodes.queryParameters();
            fail("Should have thrown Runtime Exception");
        } catch(Exception ex) {
            assertEquals("No header line found or 'id' field missing in nodes.csv", ex.getMessage());
        }
    }

    private ObjectNode createNode(String id, String name) {
        ObjectNode person1 = JsonNodeFactory.instance.objectNode();
        person1.put("id", id);
        person1.put("name", name);
        return person1;
    }
}
