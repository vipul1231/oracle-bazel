package com.example.db;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Json {

    public static JsonNode readValue(String json) {
//        return readValue(json, DefaultObjectMapper.JSON);
        return null;
    }

    public static JsonNode readValue(String json, ObjectMapper mapper) {
//        try {
//            try {
//                return json == null ? null : mapper.readValue(json, JsonNode.class);
//            } catch (JsonParseException e) {
//                if (FeatureFlag.check("UnquotedJsonPlainText")) {
//                    return mapper.readValue("\"" + json + "\"", JsonNode.class);
//                }
//                throw e;
//            }
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }

        return null;
    }

    public static JsonNode convertValue(Object value) {
//        return convertValue(value, DefaultObjectMapper.JSON);
        return null;
    }

    public static JsonNode convertValue(Object value, ObjectMapper mapper) {
        return value == null ? null : mapper.convertValue(value, JsonNode.class);
    }
}
