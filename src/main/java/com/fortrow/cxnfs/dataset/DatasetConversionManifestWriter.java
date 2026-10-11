package com.fortrow.cxnfs.dataset;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public final class DatasetConversionManifestWriter {
    public Path write(DatasetConversionPlan plan, Path outputPath) throws IOException {
        if (plan == null) {
            throw new IllegalArgumentException("plan is required");
        }
        if (outputPath == null) {
            throw new IllegalArgumentException("outputPath is required");
        }
        if (outputPath.getParent() != null) {
            Files.createDirectories(outputPath.getParent());
        }
        Files.writeString(outputPath, toJson(plan), StandardCharsets.UTF_8);
        return outputPath;
    }

    public String toJson(DatasetConversionPlan plan) {
        StringBuilder json = new StringBuilder();
        json.append("{\n");
        appendField(json, "input_dir", plan.inputDirectory().toString(), true, 1);
        appendField(json, "dataset_name", plan.datasetName(), true, 1);
        appendField(json, "source_format", plan.sourceFormat().cliName(), true, 1);
        appendField(json, "target_format", plan.targetFormat().cliName(), true, 1);
        appendNumberField(json, "target_shard_size_bytes", plan.targetShardSizeBytes(), true, 1);
        appendNumberField(json, "total_source_bytes", plan.totalSourceBytes(), true, 1);
        indent(json, 1).append("\"source_files\": [\n");
        for (int i = 0; i < plan.sourceFiles().size(); i++) {
            DatasetSourceFile source = plan.sourceFiles().get(i);
            indent(json, 2).append("{");
            json.append("\"relative_path\": \"").append(escape(source.relativePath())).append("\", ");
            json.append("\"size_bytes\": ").append(source.sizeBytes()).append("}");
            json.append(i + 1 == plan.sourceFiles().size() ? "\n" : ",\n");
        }
        indent(json, 1).append("],\n");
        indent(json, 1).append("\"shards\": [\n");
        for (int i = 0; i < plan.shards().size(); i++) {
            MdsShardPlanEntry shard = plan.shards().get(i);
            indent(json, 2).append("{\n");
            appendField(json, "shard_name", shard.shardName(), true, 3);
            appendNumberField(json, "size_bytes", shard.sizeBytes(), true, 3);
            indent(json, 3).append("\"source_files\": [");
            for (int j = 0; j < shard.sourceFiles().size(); j++) {
                DatasetSourceFile source = shard.sourceFiles().get(j);
                json.append("\"").append(escape(source.relativePath())).append("\"");
                if (j + 1 < shard.sourceFiles().size()) {
                    json.append(", ");
                }
            }
            json.append("]\n");
            indent(json, 2).append("}");
            json.append(i + 1 == plan.shards().size() ? "\n" : ",\n");
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

    private static StringBuilder indent(StringBuilder json, int depth) {
        return json.append("  ".repeat(depth));
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
