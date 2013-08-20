package org.neo4j.importer;

import com.sun.jersey.api.client.ClientResponse;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

import java.util.HashMap;
import java.util.Map;

public class CreateNodesResponse {
    private int status;
    private JsonNode response;

    public CreateNodesResponse(int status, JsonNode response) {

        this.status = status;
        this.response = response;
    }

    public int getStatus() {
        return status;
    }

    public String getOutput() {
        return response.toString();
    }

    public Map<String, Long> nodeMappings() {
        Map<String, Long> nodeMappings = new HashMap<String, Long>();

        for (JsonNode mappingAsJsonNode : response.get("data")) {
            ArrayNode  mapping = (ArrayNode) mappingAsJsonNode;
            nodeMappings.put(mapping.get(0).asText(), mapping.get(1).asLong());
        }
        return nodeMappings;
    }
}
