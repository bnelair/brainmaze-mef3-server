"""
Test different data access patterns to identify potential performance issues.
"""
import pytest
from bnel_mef3_server.server.file_manager import FileManager
from tests.conftest import mef3_file


def forward_sequential_access(file_manager, file_path, num_chunks=10):
    """Access chunks in forward sequential order (0, 1, 2, ...)"""
    for i in range(num_chunks):
        _ = list(file_manager.get_signal_segment(file_path, i))


def backward_sequential_access(file_manager, file_path, num_chunks=10):
    """Access chunks in backward sequential order (9, 8, 7, ...)"""
    for i in range(num_chunks - 1, -1, -1):
        _ = list(file_manager.get_signal_segment(file_path, i))


def random_access(file_manager, file_path, num_chunks=10):
    """Access chunks in random order"""
    # Use a fixed seed for reproducibility, but vary the pattern
    import random
    indices = list(range(num_chunks))
    random.Random(42).shuffle(indices)
    for i in indices:
        _ = list(file_manager.get_signal_segment(file_path, i))


def back_and_forth_access(file_manager, file_path):
    """Access chunks in back-and-forth pattern (viewer-like)"""
    # Simulate a viewer paging forward then backward
    # Forward: 0, 1, 2, 3, 4
    for i in range(5):
        _ = list(file_manager.get_signal_segment(file_path, i))
    # Backward: 4, 3, 2, 1, 0
    for i in range(4, -1, -1):
        _ = list(file_manager.get_signal_segment(file_path, i))


@pytest.mark.benchmark
def test_forward_sequential_with_prefetch(benchmark, mef3_file):
    """Benchmark forward sequential access WITH prefetching."""
    fm = FileManager(n_prefetch=3, cache_capacity_multiplier=5, max_workers=4)
    fm.open_file(mef3_file)
    fm.set_signal_segment_size(mef3_file, seconds=60)
    benchmark(forward_sequential_access, fm, mef3_file, 10)
    fm.shutdown()


@pytest.mark.benchmark
def test_forward_sequential_no_prefetch(benchmark, mef3_file):
    """Benchmark forward sequential access WITHOUT prefetching."""
    fm = FileManager(n_prefetch=0, cache_capacity_multiplier=0)
    fm.open_file(mef3_file)
    fm.set_signal_segment_size(mef3_file, seconds=60)
    benchmark(forward_sequential_access, fm, mef3_file, 10)
    fm.shutdown()


@pytest.mark.benchmark
def test_backward_sequential_with_prefetch(benchmark, mef3_file):
    """Benchmark backward sequential access WITH prefetching."""
    fm = FileManager(n_prefetch=3, cache_capacity_multiplier=5, max_workers=4)
    fm.open_file(mef3_file)
    fm.set_signal_segment_size(mef3_file, seconds=60)
    benchmark(backward_sequential_access, fm, mef3_file, 10)
    fm.shutdown()


@pytest.mark.benchmark
def test_backward_sequential_no_prefetch(benchmark, mef3_file):
    """Benchmark backward sequential access WITHOUT prefetching."""
    fm = FileManager(n_prefetch=0, cache_capacity_multiplier=0)
    fm.open_file(mef3_file)
    fm.set_signal_segment_size(mef3_file, seconds=60)
    benchmark(backward_sequential_access, fm, mef3_file, 10)
    fm.shutdown()


@pytest.mark.benchmark
def test_random_access_with_prefetch(benchmark, mef3_file):
    """Benchmark random access WITH prefetching."""
    fm = FileManager(n_prefetch=3, cache_capacity_multiplier=5, max_workers=4)
    fm.open_file(mef3_file)
    fm.set_signal_segment_size(mef3_file, seconds=60)
    benchmark(random_access, fm, mef3_file, 10)
    fm.shutdown()


@pytest.mark.benchmark
def test_random_access_no_prefetch(benchmark, mef3_file):
    """Benchmark random access WITHOUT prefetching."""
    fm = FileManager(n_prefetch=0, cache_capacity_multiplier=0)
    fm.open_file(mef3_file)
    fm.set_signal_segment_size(mef3_file, seconds=60)
    benchmark(random_access, fm, mef3_file, 10)
    fm.shutdown()


@pytest.mark.benchmark
def test_back_and_forth_with_prefetch(benchmark, mef3_file):
    """Benchmark back-and-forth access (viewer-like) WITH prefetching."""
    fm = FileManager(n_prefetch=3, cache_capacity_multiplier=5, max_workers=4)
    fm.open_file(mef3_file)
    fm.set_signal_segment_size(mef3_file, seconds=60)
    benchmark(back_and_forth_access, fm, mef3_file)
    fm.shutdown()


@pytest.mark.benchmark
def test_back_and_forth_no_prefetch(benchmark, mef3_file):
    """Benchmark back-and-forth access (viewer-like) WITHOUT prefetching."""
    fm = FileManager(n_prefetch=0, cache_capacity_multiplier=0)
    fm.open_file(mef3_file)
    fm.set_signal_segment_size(mef3_file, seconds=60)
    benchmark(back_and_forth_access, fm, mef3_file)
    fm.shutdown()
