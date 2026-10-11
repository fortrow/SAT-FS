package com.fortrow.cxnfs.hash;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

final class HashingTest {
    @Test
    void transferHashSupportsLargeEntrySizes() {
        ContentHash entryHash = ContentHash.of(new byte[ContentHash.BYTE_LENGTH]);

        ContentHash large = Hashing.transfer(List.of(entryHash), List.of(5L * 1024L * 1024L * 1024L));
        ContentHash small = Hashing.transfer(List.of(entryHash), List.of(1024L));

        assertEquals(ContentHash.BYTE_LENGTH, large.bytes().length);
        assertNotEquals(large, small);
    }
}
