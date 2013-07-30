package org.neo4j.importer;

import au.com.bytecode.opencsv.CSVReader;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Nodes {
    private final File nodesPath;

    public Nodes(File nodesPath) {
        this.nodesPath = nodesPath;
    }

    public ArrayNode queryParameters() {
        ArrayNode nodes = JsonNodeFactory.instance.arrayNode();

        try {
            CSVReader reader = new CSVReader(new FileReader(nodesPath), '\t');

            String[] header = reader.readNext();
            if (header == null || !Arrays.asList(header).contains("id")) {
                throw new RuntimeException("No header line found or 'id' field missing in nodes.csv");
            }

            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                ObjectNode node = JsonNodeFactory.instance.objectNode();
                for(int i=0; i < nextLine.length; i++) {
                    node.put(header[i], nextLine[i]);
                }
                nodes.add(node);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return nodes;
    }
}
