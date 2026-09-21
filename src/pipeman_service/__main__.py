""" Runnable version of the PIPEMAN process.

Note that this runs the multiprocessing compatible version.

Usage: python -m pipeman_service
"""

from pipeman_service.boot import build_processor


if __name__ == "__main__":
    pc = build_processor()
    pc.start()
