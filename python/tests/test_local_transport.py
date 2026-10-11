from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

from cxnfs import (
    CxnfsPythonClient,
    LocalFileTransport,
    SafetensorsFileEntry,
    SafetensorsSendOperation,
    TensorMemoryTarget,
)


class TensorLike:
    def __init__(self, size):
        self.data = bytearray(size)
        self.device = "cpu"
        self.is_cuda = False

    def is_pinned(self):
        return False


class FakeTorchDtype:
    def __init__(self, name, nbytes):
        self.name = name
        self.nbytes = nbytes


class FakeTorchTensor:
    def __init__(self, nbytes, *, dtype, device, pin_memory, shape=None):
        self.data = bytearray(nbytes)
        self.dtype = dtype
        self.device = device
        self._pin_memory = pin_memory
        self.is_cuda = str(device).startswith("cuda")
        self.shape = tuple(shape) if shape is not None else (nbytes // dtype.nbytes,)
        self.copied_from = None
        self.non_blocking_copy = False

    def is_pinned(self):
        return self._pin_memory

    def to(self, device, *, non_blocking=False):
        copied = FakeTorchTensor(
            len(self.data),
            dtype=self.dtype,
            device=device,
            pin_memory=False,
            shape=self.shape,
        )
        copied.data[:] = self.data
        copied.copied_from = self
        copied.non_blocking_copy = non_blocking
        return copied

    def copy_(self, other, *, non_blocking=False):
        self.data[:] = other.data
        self.copied_from = other
        self.non_blocking_copy = non_blocking
        return self


class FakeTorchCuda:
    @staticmethod
    def is_available():
        return True


class FakeTorchModule:
    float32 = FakeTorchDtype("float32", 4)
    uint8 = FakeTorchDtype("uint8", 1)
    cuda = FakeTorchCuda()

    def __init__(self):
        self.empty_calls = []

    def empty(self, shape, *, dtype, device, pin_memory=False):
        nbytes = dtype.nbytes
        for dimension in shape:
            nbytes *= dimension
        self.empty_calls.append((tuple(shape), dtype.name, device, pin_memory))
        return FakeTorchTensor(nbytes, dtype=dtype, device=device, pin_memory=pin_memory, shape=shape)


class FakeTorchInstall:
    _missing = object()

    def __init__(self, torch_module):
        self.torch_module = torch_module
        self.previous = self._missing

    def __enter__(self):
        self.previous = sys.modules.get("torch", self._missing)
        sys.modules["torch"] = self.torch_module
        return self.torch_module

    def __exit__(self, exc_type, exc, traceback):
        if self.previous is self._missing:
            sys.modules.pop("torch", None)
        else:
            sys.modules["torch"] = self.previous


class LocalFileTransportTest(unittest.TestCase):
    def test_reads_mds_shard_range_into_bytes(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            shard = root / "datasets/train/shard.00000.mds"
            shard.parent.mkdir(parents=True)
            shard.write_bytes(b"0123456789")
            client = CxnfsPythonClient(LocalFileTransport(root))

            data = client.read_mds_shard("/datasets/train/shard.00000.mds", offset=2, length=4)

            self.assertEqual(b"2345", data)

    def test_rejects_short_mds_shard_range_before_returning_data(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            shard = root / "datasets/train/shard.00000.mds"
            shard.parent.mkdir(parents=True)
            shard.write_bytes(b"012345")
            client = CxnfsPythonClient(LocalFileTransport(root))

            with self.assertRaisesRegex(ValueError, "short local MDS shard read"):
                client.read_mds_shard("datasets/train/shard.00000.mds", offset=4, length=4)

    def test_reads_into_caller_owned_buffer(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            shard = root / "datasets/train/shard.00000.mds"
            shard.parent.mkdir(parents=True)
            shard.write_bytes(b"abcdef")
            client = CxnfsPythonClient(LocalFileTransport(root))
            destination = bytearray(3)

            bytes_read = client.read_into_buffer("datasets/train/shard.00000.mds", 1, destination)

            self.assertEqual(3, bytes_read)
            self.assertEqual(bytearray(b"bcd"), destination)

    def test_rejects_short_buffer_read_without_mutating_destination(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            shard = root / "datasets/train/shard.00000.mds"
            shard.parent.mkdir(parents=True)
            shard.write_bytes(b"abcdef")
            client = CxnfsPythonClient(LocalFileTransport(root))
            destination = bytearray(b"----")

            with self.assertRaisesRegex(ValueError, "short local MDS shard read"):
                client.read_into_buffer("datasets/train/shard.00000.mds", 4, destination, 4)

            self.assertEqual(bytearray(b"----"), destination)

    def test_reads_into_tensor_like_cpu_memory(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            shard = root / "datasets/train/shard.00000.mds"
            shard.parent.mkdir(parents=True)
            shard.write_bytes(b"tensor-bytes")
            client = CxnfsPythonClient(LocalFileTransport(root))
            tensor = TensorLike(6)

            bytes_read = client.read_into_tensor(
                "datasets/train/shard.00000.mds",
                offset=7,
                tensor=tensor,
                nbytes=5,
                target=TensorMemoryTarget.PYTORCH_CPU,
            )

            self.assertEqual(5, bytes_read)
            self.assertEqual(bytearray(b"bytes\x00"), tensor.data)

    def test_allocates_pinned_cpu_tensor_for_read_tensor(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            shard = root / "datasets/train/shard.00000.mds"
            shard.parent.mkdir(parents=True)
            shard.write_bytes(b"abcdefgh")
            client = CxnfsPythonClient(LocalFileTransport(root))
            fake_torch = FakeTorchModule()

            with FakeTorchInstall(fake_torch):
                tensor = client.read_tensor(
                    "datasets/train/shard.00000.mds",
                    offset=0,
                    nbytes=8,
                    dtype="float32",
                    shape=(2,),
                    target=TensorMemoryTarget.PYTORCH_PINNED_CPU,
                )

            self.assertEqual(bytearray(b"abcdefgh"), tensor.data)
            self.assertEqual("cpu", tensor.device)
            self.assertTrue(tensor.is_pinned())
            self.assertEqual([((2,), "float32", "cpu", True)], fake_torch.empty_calls)

    def test_stages_cuda_destination_through_pinned_cpu(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            shard = root / "datasets/train/shard.00000.mds"
            shard.parent.mkdir(parents=True)
            shard.write_bytes(b"01234567")
            client = CxnfsPythonClient(LocalFileTransport(root))
            tensor = FakeTorchTensor(
                8,
                dtype=FakeTorchModule.float32,
                device="cuda:0",
                pin_memory=False,
                shape=(2,),
            )
            fake_torch = FakeTorchModule()

            with FakeTorchInstall(fake_torch):
                bytes_read = client.read_into_tensor(
                    "datasets/train/shard.00000.mds",
                    offset=0,
                    tensor=tensor,
                    nbytes=8,
                )

            self.assertEqual(8, bytes_read)
            self.assertEqual(bytearray(b"01234567"), tensor.data)
            self.assertEqual(bytearray(b"01234567"), tensor.copied_from.data)
            self.assertTrue(tensor.copied_from.is_pinned())
            self.assertTrue(tensor.non_blocking_copy)
            self.assertEqual([((2,), "float32", "cpu", True)], fake_torch.empty_calls)

    def test_allocates_cuda_tensor_for_read_tensor(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            shard = root / "datasets/train/shard.00000.mds"
            shard.parent.mkdir(parents=True)
            shard.write_bytes(b"01234567")
            client = CxnfsPythonClient(LocalFileTransport(root))
            fake_torch = FakeTorchModule()

            with FakeTorchInstall(fake_torch):
                tensor = client.read_cuda_tensor(
                    "datasets/train/shard.00000.mds",
                    offset=0,
                    nbytes=8,
                    dtype="float32",
                    shape=(2,),
                    device="cuda:0",
                )

            self.assertEqual(bytearray(b"01234567"), tensor.data)
            self.assertEqual("cuda:0", tensor.device)
            self.assertTrue(tensor.is_cuda)
            self.assertTrue(tensor.copied_from.is_pinned())
            self.assertTrue(tensor.non_blocking_copy)
            self.assertEqual([((2,), "float32", "cpu", True)], fake_torch.empty_calls)

    def test_write_tensor_writes_from_readable_memory(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            client = CxnfsPythonClient(LocalFileTransport(root))
            source = bytearray(b"checkpoint-bytes")

            result = client.write_tensor("/outputs/model.safetensors", source, len(source))

            self.assertEqual(len(source), result.bytes_sent)
            self.assertEqual(source, (root / "outputs/model.safetensors").read_bytes())

    def test_write_tensor_rejects_short_source_memory(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            client = CxnfsPythonClient(LocalFileTransport(root))

            with self.assertRaisesRegex(ValueError, "readable source size"):
                client.write_tensor("/outputs/model.safetensors", bytearray(b"short"), 64)

    def test_safetensors_send_revalidates_manifest_before_publish(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "outputs"
            output.mkdir()
            (output / "model.safetensors").write_bytes(b"changed")
            operation = SafetensorsSendOperation(
                str(output),
                ("model.safetensors",),
                ("model.safetensors",),
                file_entries=(SafetensorsFileEntry("model.safetensors", 7, "0" * 64, True),),
            )

            with self.assertRaisesRegex(ValueError, "hash changed"):
                LocalFileTransport(root).send_safetensors_output(operation)

    def test_rejects_path_escape(self):
        with TemporaryDirectory() as directory:
            client = CxnfsPythonClient(LocalFileTransport(directory))

            with self.assertRaisesRegex(ValueError, "parent"):
                client.read_mds_shard("../outside.mds")


if __name__ == "__main__":
    unittest.main()
