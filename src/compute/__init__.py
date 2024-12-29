import logging

from ._dataset import Input, Output
from .decorators import cluster_conf, compute

__all__ = [
    "Input",
    "Output",
    "cluster_conf",
    "compute",
]


# Configure the root logger minimally or not at all
logging.basicConfig(level=logging.WARNING)  # Optional: Minimal configuration for the root logger
