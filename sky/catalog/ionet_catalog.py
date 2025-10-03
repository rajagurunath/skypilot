"""IONet Catalog.

This module loads the service catalog file and can be used to query
instance types and pricing information for IONet.
"""
import typing
from typing import Dict, List, Optional, Tuple, Union

from sky.catalog import common
from sky.utils import resources_utils
from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    from sky.clouds import cloud

# Keep it synced with the frequency in
# skypilot-catalog/.github/workflows/update-ionet-catalog.yml
_PULL_FREQUENCY_HOURS = 7

_df = common.read_catalog('ionet/vms.csv',
                          pull_frequency_hours=_PULL_FREQUENCY_HOURS)

# Default configuration for IONet instances
_DEFAULT_NUM_VCPUS = 16
_DEFAULT_MEMORY_CPU_RATIO = 8  # Higher memory ratio for GPU instances


def instance_type_exists(instance_type: str) -> bool:
    return common.instance_type_exists_impl(_df, instance_type)


def validate_region_zone(
        region: Optional[str],
        zone: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if zone is not None:
        with ux_utils.print_exception_no_traceback():
            raise ValueError('IONet does not support zones.')
    return common.validate_region_zone_impl('ionet', _df, region, zone)


def get_hourly_cost(instance_type: str,
                    use_spot: bool = False,
                    region: Optional[str] = None,
                    zone: Optional[str] = None) -> float:
    """Returns the cost, or the cheapest cost among all zones for spot."""
    assert not use_spot, 'IONet does not support spot instances.'
    if zone is not None:
        with ux_utils.print_exception_no_traceback():
            raise ValueError('IONet does not support zones.')
    return common.get_hourly_cost_impl(_df, instance_type, use_spot, region,
                                       zone)


def get_vcpus_mem_from_instance_type(
        instance_type: str) -> Tuple[Optional[float], Optional[float]]:
    return common.get_vcpus_mem_from_instance_type_impl(_df, instance_type)


def get_default_instance_type(cpus: Optional[str] = None,
                              memory: Optional[str] = None,
                              disk_tier: Optional[
                                  resources_utils.DiskTier] = None,
                              region: Optional[str] = None,
                              zone: Optional[str] = None) -> Optional[str]:
    del disk_tier  # unused
    if cpus is None and memory is None:
        cpus = f'{_DEFAULT_NUM_VCPUS}+'
    if memory is None:
        memory_gb_or_ratio = f'{_DEFAULT_MEMORY_CPU_RATIO}x'
    else:
        memory_gb_or_ratio = memory
    return common.get_instance_type_for_cpus_mem_impl(_df, cpus,
                                                      memory_gb_or_ratio,
                                                      region, zone)


def get_accelerators_from_instance_type(
        instance_type: str) -> Optional[Dict[str, Union[int, float]]]:
    return common.get_accelerators_from_instance_type_impl(_df, instance_type)


def get_instance_type_for_accelerator(
        acc_name: str,
        acc_count: int,
        cpus: Optional[str] = None,
        memory: Optional[str] = None,
        use_spot: bool = False,
        region: Optional[str] = None,
        zone: Optional[str] = None) -> Tuple[Optional[List[str]], List[str]]:
    """Filter the instance types based on resource requirements.

    Returns a list of instance types satisfying the required count of
    accelerators with sorted prices and a list of candidates with fuzzy search.
    """
    if zone is not None:
        with ux_utils.print_exception_no_traceback():
            raise ValueError('IONet does not support zones.')
    return common.get_instance_type_for_accelerator_impl(df=_df,
                                                         acc_name=acc_name,
                                                         acc_count=acc_count,
                                                         cpus=cpus,
                                                         memory=memory,
                                                         use_spot=use_spot,
                                                         region=region,
                                                         zone=zone)


def regions() -> List['cloud.Region']:
    """Returns all available regions for IONet."""
    return common.get_region_zones(_df, use_spot=False)


def get_region_zones_for_instance_type(instance_type: str,
                                       use_spot: bool) -> List['cloud.Region']:
    """Returns regions available for the given instance type."""
    del use_spot  # unused
    df = _df[_df['InstanceType'] == instance_type]
    region_list = df['Region'].unique()
    ret = []
    # Import here to avoid circular import
    from sky.clouds import cloud
    for region_name in region_list:
        ret.append(cloud.Region(name=region_name, zones=None))  # IONet has no zones
    return ret


def list_accelerators(
        gpus_only: bool = True,
        name_filter: Optional[str] = None,
        region_filter: Optional[str] = None,
        quantity_filter: Optional[int] = None,
        case_sensitive: bool = True,
        all_regions: bool = False,
        require_price: bool = True
) -> Dict[str, List[common.InstanceTypeInfo]]:
    """Returns all accelerators offered by IONet."""
    del require_price  # Unused.
    return common.list_accelerators_impl('IONet', _df, gpus_only, name_filter,
                                         region_filter, quantity_filter,
                                         case_sensitive, all_regions)


def get_image_id_from_tag(tag: str, region: Optional[str]) -> Optional[str]:
    """Returns the image ID from the tag."""
    return common.get_image_id_from_tag_impl(tag, region, clouds='ionet')


def is_image_tag_valid(tag: str, region: Optional[str]) -> bool:
    """Returns True if the image tag is valid for the region.""" 
    return common.is_image_tag_valid_impl(tag, region, clouds='ionet')


def get_default_image_tag(gen_version: str, region: Optional[str]) -> Optional[str]:
    """Returns the default image tag for the region."""
    # IONet uses standard image types instead of custom tags
    del gen_version, region  # unused
    return 'skypilot:general'  # Default to general image type


def get_instance_type_for_cpus_mem(cpus: Optional[str] = None,
                                   memory: Optional[str] = None,
                                   region: Optional[str] = None,
                                   zone: Optional[str] = None) -> Optional[str]:
    """Returns the cheapest instance type that satisfies the given requirements."""
    if zone is not None:
        with ux_utils.print_exception_no_traceback():
            raise ValueError('IONet does not support zones.')
    return common.get_instance_type_for_cpus_mem_impl(_df, cpus, memory, region, zone)