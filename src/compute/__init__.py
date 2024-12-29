import logging
from .decorators import compute, cluster_conf
from ._dataset import Input, Output

__all__ = [
    'compute',
    'cluster_conf',
    'Input',
    'Output',
]


# Configure the root logger minimally or not at all
logging.basicConfig(level=logging.WARNING)  # Optional: Minimal configuration for the root logger
