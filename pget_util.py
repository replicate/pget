import os
import pathlib
import subprocess
import time


def launch_pget(file_name: str) -> None:
    file_path = pathlib.Path(file_name)
    file_downloading = file_path.with_suffix(file_path.suffix + ".loading")
    if file_path.exists() or file_downloading.exists():
        return
    # don't download by default when running in docker, so we don't download when generating schema
    if os.getenv("PGET") or not pathlib.Path("/.dockerenv").exists():
        url = f"https://weights.replicate.delivery/{file_name}"
        file_downloading.touch()
        subprocess.Popen(["/usr/bin/pget", url, file_name], close_fds=True)


def wait_pget(file_name: str) -> None:
    while not pathlib.Path(file_name).exists():
        time.sleep(0.05)


class PGetFile:
    """
    Usage:
    from pget_util import PGetFile
    pget_file = PGetFile("myweights.ckpt")

    import torch

    class Predictor:
        def setup(self):
            pget_file.wait()
            # use the file

    """
    proc: "None | subprocess.Popen"

    def __init__(self, fname: str) -> None:
        self.fname = fname
        self.file_path = pathlib.Path(fname)
        self.downloading = self.file_path.with_suffix(
            self.file_path.suffix + ".loading"
        )
        self.launch()

    def launch(self) -> None:
        # the file already exists or is already being downloaded
        if self.file_path.exists() or self.downloading.exists():
            return
        # don't download by default when running in docker, so we don't download when generating schema
        if os.getenv("PGET") or not pathlib.Path("/.dockerenv").exists():
            url = f"https://weights.replicate.delivery/{self.fname}"
            self.downloading.touch()
            self.proc = subprocess.Popen(
                ["/usr/bin/pget", url, self.fname], close_fds=True
            )

    def wait(self) -> None:
        # this is the same object and it was already launched
        if self.proc:
            self.proc.wait()
            return
        # launch never happened, do it now
        if not self.downloading.exists():
            self.launch()
            if self.proc:
                self.proc.wait()
            return
        # otherwise, someone else already started downloading it and we need to wait for that
        while not self.file_path.exists():
            time.sleep(0.05)
