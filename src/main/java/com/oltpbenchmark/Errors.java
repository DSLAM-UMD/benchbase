package com.oltpbenchmark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public final class Errors {
    private List<String> keyNames = new ArrayList<>();
    private final HashMap<String, Integer> errors = new HashMap<>();
    private final HashMap<String, String> examples = new HashMap<>();

    public Errors(String... keyNames) {
        this.keyNames = new ArrayList<String>(Arrays.asList(keyNames));
    }

    public List<String> getHeaders() {
        List<String> headers = new ArrayList<>(keyNames);
        headers.add("count");
        headers.add("example");
        return Collections.unmodifiableList(headers);
    }

    public List<List<String>> getRows() {
        List<List<String>> rows = new ArrayList<>();
        for (String key : errors.keySet()) {
            // Start with the key parts
            String[] keyParts = key.split(",");
            List<String> row = new ArrayList<String>(Arrays.asList(keyParts));

            // Add the key and the error count to the row
            row.add(errors.get(key).toString());
            
            // Sanitize the example before adding it to the row
            String example = examples.get(key);
            String sanitized = example.replace("\n", " ").replace("\r", "");
            sanitized = sanitized.replace("\"", "\"\"");
            if (sanitized.contains(",")) {
                sanitized = "\"" + sanitized + "\"";
            }
            row.add(sanitized);

            rows.add(row);
        }
        return rows;
    
    }

    public void extendKeyNames(String... newKeyNames) {
        if (errors.size() > 0) {
            throw new IllegalStateException("Cannot extend key names after errors have been added");
        }
        this.keyNames.addAll(List.of(newKeyNames));
    }

    public void addError(String example, List<String> keys) {
        if (keyNames.isEmpty()) {
            throw new IllegalArgumentException("Key names not set");
        }
        if (keys.size() != keyNames.size()) {
            throw new IllegalArgumentException("Invalid number of keys");
        }
        String key = String.join(",", keys);
        errors.put(key, errors.getOrDefault(key, 0) + 1);
        examples.putIfAbsent(key, example);
    }

    public void merge(Errors other) {
        if (keyNames.isEmpty()) {
            keyNames = other.keyNames;
        }
        if (!keyNames.equals(other.keyNames)) {
            throw new IllegalArgumentException("Key names do not match");
        }
        for (String key : other.errors.keySet()) {
            errors.put(key, errors.getOrDefault(key, 0) + other.errors.get(key));
            examples.putIfAbsent(key, other.examples.get(key));
        }
    }
}
