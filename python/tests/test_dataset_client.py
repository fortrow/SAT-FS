from pathlib import Path
from tempfile import TemporaryDirectory
import hashlib
import unittest

from cxnfs import (
    CxnfsBufferRead,
    CxnfsDatasetClient,
    CxnfsPathPrefixClient,
    CxnfsPythonClient,
    CxnfsTensorRead,
    SafetensorsFileEntry,
    SafetensorsSendOperation,
    SendResult,
    TensorMemoryTarget,
)


class FakeTensor:
    def __init__(self, device="cpu", pinned=False, is_cuda=False):
        self.device = device
        self._pinned = pinned
        self.is_cuda = is_cuda

    def is_pinned(self):
        return self._pinned


class RecordingTransport:
    def __init__(self):
        self.read_operations = []
        self.send_operations = []

    def read_mds_shard(self, operation):
        self.read_operations.append(operation)
        return b"sample-bytes"

    def send_safetensors_output(self, operation):
        self.send_operations.append(operation)
        return SendResult(
            file_count=len(operation.files),
            tensor_file_count=len(operation.tensor_files),
            bytes_sent=None,
        )


class RecordingTorchTransport(RecordingTransport):
    def __init__(self):
        super().__init__()
        self.tensor_reads = []
        self.tensor_read_into_operations = []
        self.buffer_read_operations = []
        self.tensor_writes = []

    def read_tensor(self, operation):
        self.tensor_reads.append(operation)
        return {"target": operation.target, "device": operation.device}

    def read_into_tensor(self, operation):
        self.tensor_read_into_operations.append(operation)
        return operation.nbytes

    def read_into_buffer(self, operation):
        self.buffer_read_operations.append(operation)
        operation.destination[:operation.nbytes] = b"x" * operation.nbytes
        return operation.nbytes

    def write_from_tensor(self, operation):
        self.tensor_writes.append(operation)
        return SendResult(file_count=1, tensor_file_count=1, bytes_sent=operation.nbytes)


class CxnfsDatasetClientTest(unittest.TestCase):
    def test_read_mds_shard_uses_native_read_operation(self):
        transport = RecordingTransport()
        client = CxnfsDatasetClient(transport)

        data = client.read_mds_shard("/datasets/train/shard.00000.mds", offset=128, length=4096)

        self.assertEqual(b"sample-bytes", data)
        self.assertEqual(1, len(transport.read_operations))
        operation = transport.read_operations[0]
        self.assertEqual("mds-shards", operation.format)
        self.assertEqual("/datasets/train/shard.00000.mds", operation.shard_path)
        self.assertEqual(128, operation.offset)
        self.assertEqual(4096, operation.length)

    def test_dataset_operations_reject_confusing_remote_paths(self):
        client = CxnfsPythonClient(RecordingTorchTransport())

        with self.assertRaisesRegex(ValueError, "parent"):
            client.read_mds_shard("/datasets/../shard.00000.mds", offset=0, length=1)
        with self.assertRaisesRegex(ValueError, "current"):
            client.read_tensor("/datasets/./shard.00000.mds", 0, 1, "uint8", (1,))
        with self.assertRaisesRegex(ValueError, "control"):
            client.read_into_buffer("/datasets/bad\nname.mds", 0, bytearray(1))
        with self.assertRaisesRegex(ValueError, "file"):
            client.write_tensor("/outputs/run/", object(), 1)

    def test_send_safetensors_output_uses_native_write_operation(self):
        transport = RecordingTransport()
        client = CxnfsDatasetClient(transport)

        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "model.safetensors").write_bytes(b"tensor-data")
            (root / "config.json").write_text("{}", encoding="utf-8")

            result = client.send_safetensors_output(root, remote_root="/outputs/run-1")

        self.assertEqual(2, result.file_count)
        self.assertEqual(1, result.tensor_file_count)
        operation = transport.send_operations[0]
        self.assertEqual("safetensors", operation.format)
        self.assertEqual("/outputs/run-1", operation.remote_root)
        self.assertEqual(("config.json", "model.safetensors"), operation.files)
        self.assertEqual(("model.safetensors",), operation.tensor_files)
        self.assertEqual(2, len(operation.file_entries))
        entries = {entry.path: entry for entry in operation.file_entries}
        self.assertEqual(len(b"tensor-data"), entries["model.safetensors"].size)
        self.assertEqual(hashlib.sha256(b"tensor-data").hexdigest(), entries["model.safetensors"].sha256)
        self.assertTrue(entries["model.safetensors"].tensor_file)
        self.assertEqual(hashlib.sha256(b"{}").hexdigest(), entries["config.json"].sha256)
        self.assertFalse(entries["config.json"].tensor_file)

    def test_safetensors_send_operation_validates_manifest_paths_and_entries(self):
        good_entry = SafetensorsFileEntry(
            "model.safetensors",
            11,
            hashlib.sha256(b"tensor-data").hexdigest(),
            True,
        )
        operation = SafetensorsSendOperation(
            "/outputs",
            ("model.safetensors",),
            ("model.safetensors",),
            file_entries=(good_entry,),
        )

        self.assertEqual(("model.safetensors",), operation.files)
        with self.assertRaisesRegex(ValueError, "relative"):
            SafetensorsSendOperation("/outputs", ("/model.safetensors",), ("/model.safetensors",))
        with self.assertRaisesRegex(ValueError, "subset"):
            SafetensorsSendOperation("/outputs", ("config.json",), ("model.safetensors",))
        with self.assertRaisesRegex(ValueError, "every file"):
            SafetensorsSendOperation(
                "/outputs",
                ("config.json", "model.safetensors"),
                ("model.safetensors",),
                file_entries=(good_entry,),
            )
        with self.assertRaisesRegex(ValueError, "parent"):
            SafetensorsSendOperation(
                "/outputs",
                ("model.safetensors",),
                ("model.safetensors",),
                file_entries=(good_entry,),
                remote_root="/outputs/../run",
            )

    def test_send_safetensors_output_requires_tensor_file(self):
        client = CxnfsDatasetClient(RecordingTransport())

        with TemporaryDirectory() as directory:
            Path(directory, "config.json").write_text("{}", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "safetensors"):
                client.send_safetensors_output(directory)

    def test_path_prefix_client_maps_local_shard_names_to_remote_namespace(self):
        transport = RecordingTransport()
        delegate = CxnfsDatasetClient(transport)
        client = CxnfsPathPrefixClient(delegate, "/datasets/train/")

        data = client.read_mds_shard("shard.00000.mds", offset=32, length=64)

        self.assertEqual(b"sample-bytes", data)
        operation = transport.read_operations[0]
        self.assertEqual("/datasets/train/shard.00000.mds", operation.shard_path)
        self.assertEqual(32, operation.offset)
        self.assertEqual(64, operation.length)

    def test_path_prefix_client_can_map_under_remote_root(self):
        transport = RecordingTransport()
        delegate = CxnfsDatasetClient(transport)
        client = CxnfsPathPrefixClient(delegate, "/")

        client.read_mds_shard("datasets/train/shard.00000.mds", offset=0, length=1)

        self.assertEqual("/datasets/train/shard.00000.mds", transport.read_operations[0].shard_path)

    def test_path_prefix_client_accepts_absolute_paths_inside_prefix(self):
        transport = RecordingTorchTransport()
        delegate = CxnfsPythonClient(transport)
        client = CxnfsPathPrefixClient(delegate, "/datasets/train")
        buffer = bytearray(4)

        client.read_mds_shard("/datasets/train/shard.00000.mds", offset=0, length=1)
        client.read_tensor("/datasets/train/tensor.00000.mds", 0, 4, "uint8", (4,))
        client.read_into_buffer("/datasets/train/preview.mds", 0, buffer)

        self.assertEqual("/datasets/train/shard.00000.mds", transport.read_operations[0].shard_path)
        self.assertEqual("/datasets/train/tensor.00000.mds", transport.tensor_reads[0].shard_path)
        self.assertEqual("/datasets/train/preview.mds", transport.buffer_read_operations[0].shard_path)

    def test_path_prefix_client_rejects_unsafe_prefix_and_relative_paths(self):
        delegate = CxnfsDatasetClient(RecordingTransport())

        with self.assertRaisesRegex(ValueError, "absolute"):
            CxnfsPathPrefixClient(delegate, "datasets/train")
        with self.assertRaisesRegex(ValueError, "parent"):
            CxnfsPathPrefixClient(delegate, "/datasets/../train")

        client = CxnfsPathPrefixClient(delegate, "/datasets/train")
        with self.assertRaisesRegex(ValueError, "relative"):
            client.read_mds_shard("/escape.mds", offset=0, length=1)
        with self.assertRaisesRegex(ValueError, "parent"):
            client.read_mds_shard("../escape.mds", offset=0, length=1)
        with self.assertRaisesRegex(ValueError, "current"):
            client.read_tensor("nested/./escape.mds", 0, 1, "uint8", (1,))

    def test_path_prefix_client_maps_tensor_reads_to_remote_namespace(self):
        transport = RecordingTorchTransport()
        delegate = CxnfsPythonClient(transport)
        client = CxnfsPathPrefixClient(delegate, "/datasets/train/")

        result = client.read_tensor(
            "shard.00000.mds",
            offset=2048,
            nbytes=4096,
            dtype="float32",
            shape=(1024,),
            target=TensorMemoryTarget.PYTORCH_PINNED_CPU,
        )

        self.assertEqual(TensorMemoryTarget.PYTORCH_PINNED_CPU, result["target"])
        operation = transport.tensor_reads[0]
        self.assertEqual("/datasets/train/shard.00000.mds", operation.shard_path)
        self.assertEqual(2048, operation.offset)
        self.assertEqual(4096, operation.nbytes)

    def test_path_prefix_client_maps_buffer_reads_and_tensor_writes(self):
        transport = RecordingTorchTransport()
        delegate = CxnfsPythonClient(transport)
        client = CxnfsPathPrefixClient(delegate, "/outputs/run-1")
        buffer = bytearray(4)
        tensor = object()

        bytes_read = client.read_into_buffer("preview.mds", 0, buffer)
        result = client.write_tensor("model.safetensors", tensor, 8192)

        self.assertEqual(4, bytes_read)
        self.assertEqual(bytearray(b"xxxx"), buffer)
        self.assertEqual(8192, result.bytes_sent)
        self.assertEqual("/outputs/run-1/preview.mds", transport.buffer_read_operations[0].shard_path)
        self.assertEqual("/outputs/run-1/model.safetensors", transport.tensor_writes[0].path)

    def test_path_prefix_send_defaults_remote_root_to_prefix(self):
        transport = RecordingTransport()
        delegate = CxnfsDatasetClient(transport)
        client = CxnfsPathPrefixClient(delegate, "/outputs/run-2")
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "model.safetensors").write_bytes(b"tensor-data")

            client.send_safetensors_output(root)

        self.assertEqual("/outputs/run-2", transport.send_operations[0].remote_root)

    def test_path_prefix_send_keeps_remote_root_override_inside_prefix(self):
        transport = RecordingTransport()
        delegate = CxnfsDatasetClient(transport)
        client = CxnfsPathPrefixClient(delegate, "/outputs/run-3")
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "model.safetensors").write_bytes(b"tensor-data")

            client.send_safetensors_output(root, remote_root="checkpoints/epoch-1")
            client.send_safetensors_output(root, remote_root="/outputs/run-3/checkpoints/epoch-2")

            with self.assertRaisesRegex(ValueError, "path_prefix"):
                client.send_safetensors_output(root, remote_root="/outputs/other-run")
            with self.assertRaisesRegex(ValueError, "parent"):
                client.send_safetensors_output(root, remote_root="../escape")

        self.assertEqual("/outputs/run-3/checkpoints/epoch-1", transport.send_operations[0].remote_root)
        self.assertEqual("/outputs/run-3/checkpoints/epoch-2", transport.send_operations[1].remote_root)

    def test_read_tensor_uses_torch_transport_memory_target(self):
        transport = RecordingTorchTransport()
        client = CxnfsPythonClient(transport)

        result = client.read_tensor(
            "/datasets/train/shard.00000.mds",
            offset=2048,
            nbytes=4096,
            dtype="float32",
            shape=(1024,),
            target=TensorMemoryTarget.PYTORCH_PINNED_CPU,
        )

        self.assertEqual(TensorMemoryTarget.PYTORCH_PINNED_CPU, result["target"])
        operation = transport.tensor_reads[0]
        self.assertEqual("mds-shards", operation.format)
        self.assertEqual(2048, operation.offset)
        self.assertEqual(4096, operation.nbytes)
        self.assertEqual("float32", operation.dtype)
        self.assertEqual((1024,), operation.shape)
        self.assertTrue(operation.non_blocking)

    def test_read_cuda_tensor_requests_cuda_target(self):
        transport = RecordingTorchTransport()
        client = CxnfsPythonClient(transport)

        result = client.read_cuda_tensor(
            "/datasets/train/shard.00000.mds",
            offset=0,
            nbytes=4096,
            dtype="float16",
            shape=(2048,),
            device="cuda:1",
        )

        self.assertEqual(TensorMemoryTarget.PYTORCH_CUDA, result["target"])
        operation = transport.tensor_reads[0]
        self.assertEqual("cuda:1", operation.device)

    def test_read_tensor_requires_torch_transport(self):
        client = CxnfsPythonClient(RecordingTransport())

        with self.assertRaisesRegex(TypeError, "CxnfsTorchTransport"):
            client.read_tensor("/datasets/train/shard.00000.mds", 0, 1024, "float32", (256,))

    def test_write_tensor_uses_direct_write_operation(self):
        transport = RecordingTorchTransport()
        client = CxnfsPythonClient(transport)
        tensor = object()

        result = client.write_tensor("/outputs/model.safetensors", tensor, 8192)

        self.assertEqual(8192, result.bytes_sent)
        operation = transport.tensor_writes[0]
        self.assertIs(tensor, operation.source)
        self.assertEqual("/outputs/model.safetensors", operation.path)

    def test_read_into_tensor_uses_caller_allocated_pinned_tensor(self):
        transport = RecordingTorchTransport()
        client = CxnfsPythonClient(transport)
        tensor = FakeTensor(pinned=True)

        bytes_read = client.read_into_tensor(
            "/datasets/train/shard.00000.mds",
            offset=512,
            tensor=tensor,
            nbytes=2048,
        )

        self.assertEqual(2048, bytes_read)
        operation = transport.tensor_read_into_operations[0]
        self.assertIs(tensor, operation.destination.tensor)
        self.assertEqual(TensorMemoryTarget.PYTORCH_PINNED_CPU, operation.destination.target)
        self.assertEqual("cpu", operation.destination.device)
        self.assertTrue(operation.destination.non_blocking)

    def test_read_into_tensor_infers_cuda_destination(self):
        transport = RecordingTorchTransport()
        client = CxnfsPythonClient(transport)
        tensor = FakeTensor(device="cuda:0", is_cuda=True)

        bytes_read = client.read_into_tensor(
            "/datasets/train/shard.00000.mds",
            offset=0,
            tensor=tensor,
            nbytes=1024,
        )

        self.assertEqual(1024, bytes_read)
        operation = transport.tensor_read_into_operations[0]
        self.assertEqual(TensorMemoryTarget.PYTORCH_CUDA, operation.destination.target)
        self.assertEqual("cuda:0", operation.destination.device)

    def test_read_into_buffer_uses_writable_destination_memory(self):
        transport = RecordingTorchTransport()
        client = CxnfsPythonClient(transport)
        buffer = bytearray(4)

        bytes_read = client.read_into_buffer("/datasets/train/shard.00000.mds", 0, buffer)

        self.assertEqual(4, bytes_read)
        self.assertEqual(bytearray(b"xxxx"), buffer)
        operation = transport.buffer_read_operations[0]
        self.assertIs(buffer, operation.destination.obj)

    def test_read_into_buffers_preserves_result_order_for_batch_destinations(self):
        transport = RecordingTorchTransport()
        client = CxnfsPythonClient(transport)
        first = bytearray(2)
        second = bytearray(3)

        result = client.read_into_buffers(
            (
                CxnfsBufferRead("/datasets/train/a.mds", 0, first, 2),
                CxnfsBufferRead("/datasets/train/b.mds", 4, second, 3),
            ),
            max_workers=2,
        )

        self.assertEqual((2, 3), result)
        self.assertEqual(bytearray(b"xx"), first)
        self.assertEqual(bytearray(b"xxx"), second)
        operations = sorted(transport.buffer_read_operations, key=lambda operation: operation.shard_path)
        self.assertEqual("/datasets/train/a.mds", operations[0].shard_path)
        self.assertEqual(0, operations[0].offset)
        self.assertEqual(2, operations[0].nbytes)
        self.assertEqual("/datasets/train/b.mds", operations[1].shard_path)
        self.assertEqual(4, operations[1].offset)
        self.assertEqual(3, operations[1].nbytes)

    def test_read_into_tensors_preserves_result_order_for_batch_destinations(self):
        transport = RecordingTorchTransport()
        client = CxnfsPythonClient(transport)
        first = FakeTensor(pinned=True)
        second = FakeTensor(device="cuda:0", is_cuda=True)

        result = client.read_into_tensors(
            (
                CxnfsTensorRead("/datasets/train/a.mds", 0, first, 128),
                CxnfsTensorRead("/datasets/train/b.mds", 128, second, 256),
            ),
            max_workers=2,
        )

        self.assertEqual((128, 256), result)
        operations = sorted(transport.tensor_read_into_operations, key=lambda operation: operation.shard_path)
        self.assertEqual(TensorMemoryTarget.PYTORCH_PINNED_CPU, operations[0].destination.target)
        self.assertEqual(TensorMemoryTarget.PYTORCH_CUDA, operations[1].destination.target)
        self.assertEqual("cuda:0", operations[1].destination.device)

    def test_path_prefix_client_maps_batch_buffer_reads_to_remote_namespace(self):
        transport = RecordingTorchTransport()
        delegate = CxnfsPythonClient(transport)
        client = CxnfsPathPrefixClient(delegate, "/datasets/train")
        first = bytearray(1)
        second = bytearray(1)

        result = client.read_into_buffers(
            (
                CxnfsBufferRead("a.mds", 0, first),
                CxnfsBufferRead("nested/b.mds", 2, second),
            ),
            max_workers=1,
        )

        self.assertEqual((1, 1), result)
        self.assertEqual(
            ["/datasets/train/a.mds", "/datasets/train/nested/b.mds"],
            [operation.shard_path for operation in transport.buffer_read_operations],
        )

    def test_batch_direct_reads_reject_invalid_worker_count(self):
        client = CxnfsPythonClient(RecordingTorchTransport())

        with self.assertRaisesRegex(ValueError, "max_workers"):
            client.read_into_buffers(
                (CxnfsBufferRead("/datasets/train/a.mds", 0, bytearray(1)),),
                max_workers=0,
            )

    def test_read_into_buffer_rejects_readonly_destination(self):
        transport = RecordingTorchTransport()
        client = CxnfsPythonClient(transport)

        with self.assertRaisesRegex(ValueError, "writable"):
            client.read_into_buffer("/datasets/train/shard.00000.mds", 0, b"readonly")


if __name__ == "__main__":
    unittest.main()
