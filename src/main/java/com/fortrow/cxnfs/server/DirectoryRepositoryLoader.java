package com.fortrow.cxnfs.server;

import com.fortrow.cxnfs.store.CxnfsStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public final class DirectoryRepositoryLoader {
    public DirectoryLoadResult load(Path directory, CxnfsStore repository) throws IOException {
        return load(List.of(ServedRoot.root(directory)), repository);
    }

    public DirectoryLoadResult load(List<ServedRoot> roots, CxnfsStore repository) throws IOException {
        return load(roots, repository, null);
    }

    public DirectoryLoadResult load(
            List<ServedRoot> roots,
            CxnfsStore repository,
            SourceFileIndex sourceFileIndex
    ) throws IOException {
        if (roots == null || roots.isEmpty()) {
            throw new IllegalArgumentException("roots are required");
        }
        if (repository == null) {
            throw new IllegalArgumentException("repository is required");
        }
        validateRemotePrefixes(roots);
        validateLocalJails(roots);
        long files = 0;
        long bytes = 0;
        Set<String> loadedPaths = new LinkedHashSet<>();
        for (ServedRoot root : roots) {
            DirectoryLoadResult loaded = loadRoot(root, repository, sourceFileIndex, loadedPaths);
            files += loaded.filesLoaded();
            bytes += loaded.bytesLoaded();
        }
        repository.pruneExcept(loadedPaths);
        if (sourceFileIndex != null) {
            sourceFileIndex.retainOnly(loadedPaths);
        }
        return new DirectoryLoadResult(files, bytes);
    }

    private DirectoryLoadResult loadRoot(
            ServedRoot root,
            CxnfsStore repository,
            SourceFileIndex sourceFileIndex,
            Set<String> loadedPaths
    ) throws IOException {
        Path directory = root.localDirectory();
        if (directory == null) {
            throw new IllegalArgumentException("directory is required");
        }
        if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("directory does not exist: " + directory);
        }
        long files = 0;
        long bytes = 0;
        try (Stream<Path> paths = Files.walk(directory)) {
            for (Path path : paths
                    .filter(candidate -> Files.isRegularFile(candidate, LinkOption.NOFOLLOW_LINKS))
                    .sorted(Comparator.naturalOrder())
                    .toList()) {
                String namespacePath = namespacePath(root.remotePrefix(), directory.relativize(path));
                BasicFileAttributes attributes = Files.readAttributes(
                        path,
                        BasicFileAttributes.class,
                        LinkOption.NOFOLLOW_LINKS
                );
                long fileSize = attributes.size();
                SourceFileState sourceFileState = SourceFileState.from(attributes);
                boolean unchanged = sourceFileIndex != null
                        && sourceFileIndex.isUnchanged(namespacePath, sourceFileState);
                if (!unchanged) {
                    try (var input = Files.newInputStream(path)) {
                        repository.putStream(namespacePath, input);
                    }
                    if (sourceFileIndex != null) {
                        sourceFileIndex.record(namespacePath, sourceFileState);
                    }
                    files++;
                    bytes += fileSize;
                }
                loadedPaths.add(namespacePath);
            }
        }
        return new DirectoryLoadResult(files, bytes);
    }

    private static String namespacePath(String remotePrefix, Path relativePath) {
        String relative = relativePath.toString().replace('\\', '/');
        if (remotePrefix.equals("/")) {
            return "/" + relative;
        }
        return remotePrefix + "/" + relative;
    }

    private static void validateRemotePrefixes(List<ServedRoot> roots) {
        for (int left = 0; left < roots.size(); left++) {
            String leftPrefix = roots.get(left).remotePrefix();
            for (int right = left + 1; right < roots.size(); right++) {
                String rightPrefix = roots.get(right).remotePrefix();
                if (leftPrefix.equals(rightPrefix)) {
                    throw new IllegalArgumentException("duplicate served root prefix: " + leftPrefix);
                }
                if (isPrefixAncestor(leftPrefix, rightPrefix) || isPrefixAncestor(rightPrefix, leftPrefix)) {
                    throw new IllegalArgumentException("served root prefixes must not overlap: "
                            + leftPrefix + " and " + rightPrefix);
                }
            }
        }
    }

    private static void validateLocalJails(List<ServedRoot> roots) throws IOException {
        List<Path> realRoots = roots.stream()
                .map(ServedRoot::localDirectory)
                .map(DirectoryRepositoryLoader::realDirectory)
                .toList();
        for (int left = 0; left < realRoots.size(); left++) {
            Path leftRoot = realRoots.get(left);
            for (int right = left + 1; right < realRoots.size(); right++) {
                Path rightRoot = realRoots.get(right);
                if (leftRoot.equals(rightRoot)) {
                    throw new IllegalArgumentException("duplicate served root directory: " + leftRoot);
                }
                if (leftRoot.startsWith(rightRoot) || rightRoot.startsWith(leftRoot)) {
                    throw new IllegalArgumentException("served root directories must not overlap: "
                            + leftRoot + " and " + rightRoot);
                }
            }
        }
    }

    private static Path realDirectory(Path directory) {
        try {
            return directory.toRealPath(LinkOption.NOFOLLOW_LINKS);
        } catch (IOException e) {
            throw new IllegalArgumentException("directory does not exist: " + directory);
        }
    }

    private static boolean isPrefixAncestor(String root, String candidate) {
        if (root.equals("/")) {
            return true;
        }
        return candidate.startsWith(root + "/");
    }

    public static final class SourceFileIndex {
        private final Map<String, SourceFileState> statesByPath = new LinkedHashMap<>();

        boolean isUnchanged(String namespacePath, SourceFileState sourceFileState) {
            return sourceFileState.equals(statesByPath.get(namespacePath));
        }

        void record(String namespacePath, SourceFileState sourceFileState) {
            statesByPath.put(namespacePath, sourceFileState);
        }

        void retainOnly(Set<String> namespacePaths) {
            statesByPath.keySet().retainAll(namespacePaths);
        }
    }

    private record SourceFileState(
            long size,
            FileTime modifiedAt,
            String fileKey
    ) {
        static SourceFileState from(BasicFileAttributes attributes) {
            Object key = attributes.fileKey();
            return new SourceFileState(
                    attributes.size(),
                    attributes.lastModifiedTime(),
                    key == null ? "" : key.toString()
            );
        }
    }
}
