package com.fortrow.cxnfs.codec;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class CompressionCodecRegistryTest {
    @Test
    void defaultRegistryContainsIdentityAndLz4() {
        CompressionCodecRegistry registry = CompressionCodecRegistry.defaults();

        assertInstanceOf(IdentityCompressionCodec.class, registry.require(IdentityCompressionCodec.ID));
        assertInstanceOf(Lz4FrameCompressionCodec.class, registry.require(Lz4FrameCompressionCodec.ID));
    }

    @Test
    void rejectsDuplicateCodecIds() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new CompressionCodecRegistry(List.of(new IdentityCompressionCodec(), new IdentityCompressionCodec()))
        );
    }

    @Test
    void rejectsUnknownCodecIds() {
        assertThrows(IllegalArgumentException.class, () -> CompressionCodecRegistry.defaults().require((byte) 99));
    }
}
