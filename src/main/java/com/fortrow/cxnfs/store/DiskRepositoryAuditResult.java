package com.fortrow.cxnfs.store;

import java.nio.file.Path;

public record DiskRepositoryAuditResult(
        Path root,
        int manifestFiles,
        int containerFiles,
        int journalFiles,
        int tempFiles,
        int referencedChunks,
        int storedChunks,
        int corruptManifestFiles,
        int corruptContainerFiles,
        int corruptJournalFiles,
        int invalidManifestFiles,
        int replayableJournalFiles,
        int incompleteJournalFiles,
        int containerNameMismatches,
        int missingChunks,
        int orphanContainers,
        int namespaceConflicts
) {
    public DiskRepositoryAuditResult {
        if (root == null) {
            throw new IllegalArgumentException("root is required");
        }
        if (manifestFiles < 0
                || containerFiles < 0
                || journalFiles < 0
                || tempFiles < 0
                || referencedChunks < 0
                || storedChunks < 0
                || corruptManifestFiles < 0
                || corruptContainerFiles < 0
                || corruptJournalFiles < 0
                || invalidManifestFiles < 0
                || replayableJournalFiles < 0
                || incompleteJournalFiles < 0
                || containerNameMismatches < 0
                || missingChunks < 0
                || orphanContainers < 0
                || namespaceConflicts < 0) {
            throw new IllegalArgumentException("audit counts must be non-negative");
        }
        if (replayableJournalFiles + incompleteJournalFiles + corruptJournalFiles != journalFiles) {
            throw new IllegalArgumentException("journal categories must add up to journalFiles");
        }
    }

    public int issueCount() {
        return corruptManifestFiles
                + corruptContainerFiles
                + journalFiles
                + invalidManifestFiles
                + containerNameMismatches
                + missingChunks
                + orphanContainers
                + namespaceConflicts;
    }

    public boolean clean() {
        return issueCount() == 0;
    }

    public String toReportText() {
        String lineSeparator = System.lineSeparator();
        return "cxnfs.repository.audit.v1" + lineSeparator
                + "root=" + root + lineSeparator
                + "clean=" + clean() + lineSeparator
                + "issue_count=" + issueCount() + lineSeparator
                + "manifest_files=" + manifestFiles + lineSeparator
                + "container_files=" + containerFiles + lineSeparator
                + "journal_files=" + journalFiles + lineSeparator
                + "temp_files=" + tempFiles + lineSeparator
                + "referenced_chunks=" + referencedChunks + lineSeparator
                + "stored_chunks=" + storedChunks + lineSeparator
                + "corrupt_manifest_files=" + corruptManifestFiles + lineSeparator
                + "corrupt_container_files=" + corruptContainerFiles + lineSeparator
                + "corrupt_journal_files=" + corruptJournalFiles + lineSeparator
                + "invalid_manifest_files=" + invalidManifestFiles + lineSeparator
                + "replayable_journal_files=" + replayableJournalFiles + lineSeparator
                + "incomplete_journal_files=" + incompleteJournalFiles + lineSeparator
                + "container_name_mismatches=" + containerNameMismatches + lineSeparator
                + "missing_chunks=" + missingChunks + lineSeparator
                + "orphan_containers=" + orphanContainers + lineSeparator
                + "namespace_conflicts=" + namespaceConflicts + lineSeparator;
    }
}
