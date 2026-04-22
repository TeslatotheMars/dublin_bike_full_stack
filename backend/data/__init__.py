"""
Data utilities module for Dublin Bikes analytics.
Includes demo data generation and data availability checking.
"""

from .demo_data import generate_demo_hourly_dataset, generate_demo_stats_data
from .data_check import check_data_availability, get_data_quality_summary

__all__ = [
    'generate_demo_hourly_dataset',
    'generate_demo_stats_data',
    'check_data_availability',
    'get_data_quality_summary',
]
