

from mef_tools import MefReader, MefWriter

import numpy as np
import pathlib
import os
from datetime import datetime

import pytest
from tqdm import tqdm

from bnel_mef3_server.client import Mef3Client
import bnel_mef3_server.server

import multiprocessing
import time
import pytest
from bnel_mef3_server.server.__main__ import main as server_entrypoint


@pytest.fixture(scope="function")
def launch_server_process():
    """
    Launches the gRPC server main() in a separate process.
    """
    # Initialize process targeting the main entrypoint
    proc = multiprocessing.Process(target=server_entrypoint, daemon=True)
    proc.start()

    # Wait briefly for the server to bind the port
    time.sleep(1)

    yield

    # Terminate the process (triggers handle_sigterm in main)
    proc.terminate()
    proc.join()


@pytest.mark.slow
def test_real_life_data(tmp_path):
    pth = str(tmp_path)
    pth_mef = os.path.join(pth, "big_data_demo.mefd")

    n_channels = 64
    fs = 512
    data_len_s = 60 * 60

    wrt = MefWriter(pth_mef, overwrite=True)
    wrt.mef_block_len = 10000
    wrt.max_nans_written = 0

    for idx in tqdm(range(n_channels)):
        chname = f"chan_{idx+1:03d}"
        x = np.random.randn(data_len_s * fs)
        wrt.write_data(x, chname, (datetime.now().timestamp() - 3600*24*100)*1e6, fs)


    cl = Mef3Client("localhost:50051")
    cl.open_file(pth_mef)
    fi = cl.get_file_info(pth_mef)

    channels = fi["channel_names"]
    cl.set_active_channels(pth_mef, channels)


    cl.set_active_channels(pth_mef, channels[:32])

    cl.set_signal_segment_size(pth_mef, 1*60)

    cl.set_signal_segment_size(pth_mef, 5*60)

    n_segnments = (data_len_s) // (5*60)

    x = cl.get_signal_segment(pth_mef, 0)
    assert x[0].shape[0] == 1 * 60 * fs

    cl.set_signal_segment_size(pth_mef, 1*60)

    for seg_idx in tqdm(range(n_segnments)):
        x = cl.get_signal_segment(pth_mef, seg_idx)
        for ch_idx in range(n_channels):
            arr = x[ch_idx]
            assert arr.shape[0] == 1*60*fs