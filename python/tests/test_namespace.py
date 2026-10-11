import unittest

from cxnfs.namespace import (
    join_namespace_path,
    normalize_namespace_file_path,
    normalize_namespace_path,
    normalize_relative_namespace_path,
    require_namespace_path_inside_prefix,
)


class NamespacePathTest(unittest.TestCase):
    def test_normalizes_remote_paths_without_resolving_traversal(self):
        self.assertEqual("/datasets/train/shard.mds", normalize_namespace_path("datasets//train/shard.mds/"))
        self.assertEqual("datasets/train", normalize_relative_namespace_path("datasets//train"))
        self.assertEqual("/outputs/run/model.safetensors", normalize_namespace_file_path("/outputs/run/model.safetensors"))

        with self.assertRaisesRegex(ValueError, "parent"):
            normalize_namespace_path("/datasets/../train")
        with self.assertRaisesRegex(ValueError, "current"):
            normalize_relative_namespace_path("datasets/./train")
        with self.assertRaisesRegex(ValueError, "control"):
            normalize_namespace_path("/datasets/bad\nname")
        with self.assertRaisesRegex(ValueError, "file"):
            normalize_namespace_file_path("/outputs/run/")

    def test_joins_and_bounds_paths_under_prefix(self):
        self.assertEqual("/datasets/train/shard.mds", join_namespace_path("/datasets/train", "shard.mds"))

        require_namespace_path_inside_prefix("/datasets/train", "/datasets/train/shard.mds")
        with self.assertRaisesRegex(ValueError, "path_prefix"):
            require_namespace_path_inside_prefix("/datasets/train", "/datasets/other/shard.mds")


if __name__ == "__main__":
    unittest.main()
