package com.fortrow.cxnfs.namespace;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class NamespacePathTest {
    @Test
    void normalizesNamespacePathsWithoutResolvingTraversal() {
        assertEquals("/", NamespacePath.normalizeDirectory(""));
        assertEquals("/datasets/train/shard.mds", NamespacePath.normalize("datasets//train/shard.mds/"));

        assertThrows(IllegalArgumentException.class, () -> NamespacePath.normalize("/datasets/./train"));
        assertThrows(IllegalArgumentException.class, () -> NamespacePath.normalize("/datasets/../train"));
        assertThrows(IllegalArgumentException.class, () -> NamespacePath.normalize("/datasets/bad\nname"));
    }

    @Test
    void checksAdmittedTreeBoundariesAfterCanonicalNormalization() {
        NamespacePath.requireUnderRoot("/outputs/run-1/", "outputs//run-1/model.safetensors");

        assertThrows(
                IllegalArgumentException.class,
                () -> NamespacePath.requireUnderRoot("/outputs/run-1", "/outputs/run-10/model.safetensors")
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> NamespacePath.requireUnderRoot("/outputs/run-1", "/outputs/run-1/../other")
        );
    }
}
