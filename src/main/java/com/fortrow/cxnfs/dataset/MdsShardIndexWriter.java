package com.fortrow.cxnfs.dataset;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class MdsShardIndexWriter {
    public Path write(DatasetConversionPlan plan, List<MdsShardArtifact> shards, Path indexPath) throws IOException {
        if (plan == null) {
            throw new IllegalArgumentException("plan is required");
        }
        if (shards == null || shards.isEmpty()) {
            throw new IllegalArgumentException("shards are required");
        }
        if (indexPath == null) {
            throw new IllegalArgumentException("indexPath is required");
        }
        if (indexPath.getParent() != null) {
            Files.createDirectories(indexPath.getParent());
        }
        Files.writeString(indexPath, toJson(plan, shards), StandardCharsets.UTF_8);
        return indexPath;
    }

    public String toJson(DatasetConversionPlan plan, List<MdsShardArtifact> shards) {
        StringBuilder json = new StringBuilder();
        json.append("{\n");
        indent(json, 1).append("\"version\": 2,\n");
        indent(json, 1).append("\"shards\": [\n");
        for (int i = 0; i < shards.size(); i++) {
            MdsShardArtifact shard = shards.get(i);
            indent(json, 2).append("{\n");
            appendArrayField(json, "column_encodings", List.of("\"bytes\"", "\"str\""), true, 3);
            appendArrayField(json, "column_names", List.of("\"data\"", "\"path\""), true, 3);
            indent(json, 3).append("\"column_sizes\": [null, null],\n");
            indent(json, 3).append("\"compression\": null,\n");
            appendField(json, "format", "mds", true, 3);
            indent(json, 3).append("\"hashes\": [],\n");
            indent(json, 3).append("\"raw_data\": {");
            json.append("\"basename\": \"").append(escape(shard.shardName())).append("\", ");
            json.append("\"bytes\": ").append(shard.byteSize()).append(", ");
            json.append("\"hashes\": {}},\n");
            appendNumberField(json, "samples", shard.sampleCount(), true, 3);
            appendNumberField(json, "size_limit", plan.targetShardSizeBytes(), true, 3);
            appendNumberField(json, "version", 2, true, 3);
            indent(json, 3).append("\"zip_data\": null\n");
            indent(json, 2).append("}");
            json.append(i + 1 == shards.size() ? "\n" : ",\n");
        }
        indent(json, 1).append("]\n");
        json.append("}\n");
        return json.toString();
    }

    private static void appendField(StringBuilder json, String name, String value, boolean comma, int depth) {
        indent(json, depth).append("\"").append(name).append("\": \"").append(escape(value)).append("\"");
        json.append(comma ? ",\n" : "\n");
    }

    private static void appendNumberField(StringBuilder json, String name, long value, boolean comma, int depth) {
        indent(json, depth).append("\"").append(name).append("\": ").append(value);
        json.append(comma ? ",\n" : "\n");
    }

    private static void appendArrayField(
            StringBuilder json,
            String name,
            List<String> encodedValues,
            boolean comma,
            int depth
    ) {
        indent(json, depth).append("\"").append(name).append("\": [");
        for (int i = 0; i < encodedValues.size(); i++) {
            json.append(encodedValues.get(i));
            if (i + 1 < encodedValues.size()) {
                json.append(", ");
            }
        }
        json.append("]");
        json.append(comma ? ",\n" : "\n");
    }

    private static StringBuilder indent(StringBuilder json, int depth) {
        return json.append("  ".repeat(depth));
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
