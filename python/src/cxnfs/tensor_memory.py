from __future__ import annotations

from typing import Any


def require_torch():
    try:
        import torch  # type: ignore[import-not-found]
    except ImportError as exc:
        raise RuntimeError("PyTorch is required for allocated tensor reads") from exc
    return torch


def torch_dtype(torch, dtype: str):
    normalized = dtype.removeprefix("torch.")
    value = getattr(torch, normalized, None)
    if value is None:
        raise ValueError(f"unsupported PyTorch dtype: {dtype}")
    return value


def torch_empty(torch, shape: tuple[int, ...], *, dtype, device: str, pin_memory: bool):
    active_pin_memory = bool(pin_memory and torch_pinned_memory_available(torch))
    try:
        return torch.empty(shape, dtype=dtype, device=device, pin_memory=active_pin_memory)
    except TypeError:
        tensor = torch.empty(shape, dtype=dtype, device=device)
        if active_pin_memory:
            pin_memory_method = getattr(tensor, "pin_memory", None)
            if not callable(pin_memory_method):
                raise
            tensor = pin_memory_method()
        return tensor


def torch_pinned_memory_available(torch) -> bool:
    cuda = getattr(torch, "cuda", None)
    is_available = getattr(cuda, "is_available", None)
    return bool(callable(is_available) and is_available())


def tensor_shape(tensor: Any) -> tuple[int, ...]:
    shape = getattr(tensor, "shape", None)
    if shape is None:
        size = getattr(tensor, "size", None)
        shape = size() if callable(size) else None
    if shape is None:
        raise TypeError("tensor destination must expose shape")
    result = tuple(int(dimension) for dimension in shape)
    if not result or any(dimension <= 0 for dimension in result):
        raise ValueError("tensor destination shape must be non-empty and positive")
    return result


def require_tensor_read_nbytes_match(nbytes: int, dtype, shape: tuple[int, ...]) -> None:
    expected = shape_nbytes(dtype, shape)
    if nbytes != expected:
        raise ValueError(f"tensor read byte count must match dtype and shape: {nbytes} != {expected}")


def shape_nbytes(dtype, shape: tuple[int, ...]) -> int:
    element_size = dtype_nbytes(dtype)
    elements = 1
    for dimension in shape:
        elements *= int(dimension)
    return elements * element_size


def dtype_nbytes(dtype) -> int:
    for name in ("nbytes", "itemsize"):
        value = getattr(dtype, name, None)
        if value is not None:
            return int(value)
    raise TypeError("PyTorch dtype must expose byte size")


def tensor_writable_memoryview(tensor: Any) -> memoryview:
    direct = try_memoryview(tensor)
    if direct is not None and not direct.readonly:
        return direct.cast("B")

    numpy_method = getattr(tensor, "numpy", None)
    if callable(numpy_method):
        view = try_memoryview(numpy_method())
        if view is not None and not view.readonly:
            return view.cast("B")

    data = getattr(tensor, "data", None)
    view = try_memoryview(data)
    if view is not None and not view.readonly:
        return view.cast("B")

    raise TypeError("tensor destination does not expose writable CPU memory")


def readable_source_memoryview(source: Any) -> memoryview:
    direct = try_memoryview(source)
    if direct is not None:
        return direct.cast("B")

    detach = getattr(source, "detach", None)
    detached = detach() if callable(detach) else source
    cpu_method = getattr(detached, "cpu", None)
    cpu_source = cpu_method() if callable(cpu_method) else detached
    numpy_source = getattr(cpu_source, "numpy", None)
    if callable(numpy_source):
        view = try_memoryview(numpy_source())
        if view is not None:
            return view.cast("B")

    data = getattr(source, "data", None)
    view = try_memoryview(data)
    if view is not None:
        return view.cast("B")

    raise TypeError("source does not expose readable memory")


def try_memoryview(value: Any) -> memoryview | None:
    if value is None:
        return None
    try:
        return memoryview(value)
    except TypeError:
        return None


def copy_staging_to_tensor(staging: Any, tensor: Any, *, non_blocking: bool) -> None:
    copy_method = getattr(tensor, "copy_", None)
    if not callable(copy_method):
        raise TypeError("cuda tensor destination must expose copy_")
    try:
        copy_method(staging, non_blocking=non_blocking)
    except TypeError:
        if non_blocking:
            raise
        copy_method(staging)
