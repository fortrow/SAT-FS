package com.fortrow.cxnfs.codec;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class CompressionCodecRegistry {
    private final Map<Byte, CompressionCodec> codecs;

    public CompressionCodecRegistry(List<CompressionCodec> codecs) {
        if (codecs == null || codecs.isEmpty()) {
            throw new IllegalArgumentException("at least one codec is required");
        }
        Map<Byte, CompressionCodec> byId = new LinkedHashMap<>();
        for (CompressionCodec codec : codecs) {
            CompressionCodec previous = byId.put(codec.id(), codec);
            if (previous != null) {
                throw new IllegalArgumentException("duplicate compression codec id: " + codec.id());
            }
        }
        this.codecs = Map.copyOf(byId);
    }

    public static CompressionCodecRegistry defaults() {
        return new CompressionCodecRegistry(List.of(
                new IdentityCompressionCodec(),
                new Lz4FrameCompressionCodec()
        ));
    }

    public CompressionCodec require(byte id) {
        CompressionCodec codec = codecs.get(id);
        if (codec == null) {
            throw new IllegalArgumentException("unknown compression codec id: " + id);
        }
        return codec;
    }
}
