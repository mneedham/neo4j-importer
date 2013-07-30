package org.neo4j.importer;

import au.com.bytecode.opencsv.CSVReader;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import static java.util.Arrays.*;

public class Relationships {
    private File relationshipsPath;

    public Relationships(File relationshipsPath) {
        this.relationshipsPath = relationshipsPath;
    }

    public List<Map<String, Object>> get() {
        List<Map<String, Object>> relationships = new ArrayList<Map<String, Object>>();

        try {
            CSVReader reader = new CSVReader(new FileReader(relationshipsPath), '\t');

            String[] header = reader.readNext();
            if (header == null || !asList(header).contains("from") || !asList(header).contains("to") || !asList(header).contains("type") ) {
                throw new RuntimeException("No header line found or 'from', 'to', 'type' fields missing in nodes.csv");
            }

            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                Map<String, Object> relationship = new HashMap<String, Object>();
                for(int i=0; i < nextLine.length; i++) {
                    relationship.put(header[i], nextLine[i]);
                }
                relationships.add(relationship);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return relationships;
    }
}
