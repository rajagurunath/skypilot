"""IONet cloud implementation for SkyPilot."""
import typing
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

from sky import catalog
from sky import clouds
from sky import sky_logging
from sky.adaptors import common as adaptors_common
from sky.provision.ionet import utils as ionet_utils
from sky.utils import registry
from sky.utils import resources_utils

if typing.TYPE_CHECKING:
    # Renaming to avoid shadowing variables.
    from sky import resources as resources_lib
    from sky.utils import volume as volume_lib
else:
    requests = adaptors_common.LazyImport('requests')

logger = sky_logging.init_logger(__name__)

# Minimum set of files under ~/.ionet that grant IONet access.
_CREDENTIAL_FILES = [
    'api_key',
]


@registry.CLOUD_REGISTRY.register
class IONet(clouds.Cloud):
    """IONet GPU Cloud Provider."""

    _REPR = 'IONet'
    
    # IONet deployment names have reasonable limits
    _MAX_CLUSTER_NAME_LEN_LIMIT = 50
    
    # IONet capabilities and limitations
    _CLOUD_UNSUPPORTED_FEATURES = {
        clouds.CloudImplementationFeatures.STOP: 'IONet does not support stopping VMs.',
        clouds.CloudImplementationFeatures.CLONE_DISK_FROM_CLUSTER: f'Migrating disk is currently not supported on {_REPR}.',
        clouds.CloudImplementationFeatures.SPOT_INSTANCE: f'Spot instances are not supported in {_REPR}.',
        clouds.CloudImplementationFeatures.IMAGE_ID: f'Specifying image ID is not supported in {_REPR}.',
        clouds.CloudImplementationFeatures.CUSTOM_DISK_TIER: f'Custom disk tiers are not supported in {_REPR}.',
        clouds.CloudImplementationFeatures.CUSTOM_NETWORK_TIER: f'Custom network tier is currently not supported in {_REPR}.',
        clouds.CloudImplementationFeatures.HOST_CONTROLLERS: f'Host controllers are not supported in {_REPR}.',
        clouds.CloudImplementationFeatures.HIGH_AVAILABILITY_CONTROLLERS: f'High availability controllers are not supported on {_REPR}.',
        clouds.CloudImplementationFeatures.CUSTOM_MULTI_NETWORK: f'Customized multiple network interfaces are not supported in {_REPR}.',
        clouds.CloudImplementationFeatures.MULTI_NODE: f'Multi-node clusters are not yet supported in {_REPR}.',
    }

    PROVISIONER_VERSION = clouds.ProvisionerVersion.SKYPILOT
    STATUS_VERSION = clouds.StatusVersion.SKYPILOT

    @classmethod
    def _unsupported_features_for_resources(
        cls, resources: 'resources_lib.Resources'
    ) -> Dict[clouds.CloudImplementationFeatures, str]:
        del resources  # unused
        return cls._CLOUD_UNSUPPORTED_FEATURES

    @classmethod
    def _check_credentials(cls) -> Tuple[bool, Optional[str]]:
        """Check if IONet credentials are properly configured."""
        try:
            api_key = ionet_utils.get_api_key()
            if not api_key:
                return False, ('IONet API key not found. Please run: '
                             'mkdir -p ~/.ionet && echo "your-api-key" > ~/.ionet/api_key')
            
            # Test API key with a simple call
            client = ionet_utils.IONetClient()
            client.test_connection()
            return True, None
        except Exception as e:
            return False, f'IONet credential check failed: {str(e)}'

    def get_credential_file_mounts(self) -> Dict[str, str]:
        """Return credential files to mount on instances."""
        return {f'~/.ionet/{filename}': f'~/.ionet/{filename}' 
                for filename in _CREDENTIAL_FILES}

    @classmethod
    def max_cluster_name_length(cls) -> Optional[int]:
        return cls._MAX_CLUSTER_NAME_LEN_LIMIT

    @classmethod
    def regions_with_offering(cls, instance_type: str,
                              accelerators: Optional[Dict[str, int]],
                              use_spot: bool, region: Optional[str],
                              zone: Optional[str]) -> List[clouds.Region]:
        assert zone is None, 'IONet does not support zones.'
        del accelerators, zone  # unused
        if use_spot:
            return []
        
        regions = catalog.get_region_zones_for_instance_type(
            instance_type, use_spot, 'ionet')

        if region is not None:
            regions = [r for r in regions if r.name == region]
        return regions

    @classmethod
    def zones_provision_loop(
        cls,
        *,
        region: str,
        num_nodes: int,
        instance_type: str,
        accelerators: Optional[Dict[str, int]] = None,
        use_spot: bool = False,
    ) -> Iterator[None]:
        del num_nodes  # unused
        regions = cls.regions_with_offering(instance_type,
                                            accelerators,
                                            use_spot,
                                            region=region,
                                            zone=None)
        for r in regions:
            assert r.zones is None, r
            yield r.zones

    def instance_type_to_hourly_cost(self,
                                     instance_type: str,
                                     use_spot: bool,
                                     region: Optional[str] = None,
                                     zone: Optional[str] = None) -> float:
        return catalog.get_hourly_cost(instance_type,
                                       use_spot=use_spot,
                                       region=region,
                                       zone=zone,
                                       clouds='ionet')

    def accelerators_to_hourly_cost(self,
                                    accelerators: Dict[str, int],
                                    use_spot: bool,
                                    region: Optional[str] = None,
                                    zone: Optional[str] = None) -> float:
        del accelerators, use_spot, region, zone  # unused
        # IONet includes accelerators as part of the instance type.
        return 0.0

    def get_egress_cost(self, num_gigabytes: float) -> float:
        # IONet doesn't charge for egress
        return 0.0

    def __repr__(self):
        return 'IONet'

    @classmethod
    def get_default_instance_type(
            cls,
            cpus: Optional[str] = None,
            memory: Optional[str] = None,
            disk_tier: Optional['resources_utils.DiskTier'] = None,
            region: Optional[str] = None,
            zone: Optional[str] = None) -> Optional[str]:
        return catalog.get_default_instance_type(cpus=cpus,
                                                 memory=memory,
                                                 disk_tier=disk_tier,
                                                 region=region,
                                                 zone=zone,
                                                 clouds='ionet')

    @classmethod
    def get_accelerators_from_instance_type(
        cls,
        instance_type: str,
    ) -> Optional[Dict[str, Union[int, float]]]:
        return catalog.get_accelerators_from_instance_type(instance_type,
                                                           clouds='ionet')

    @classmethod
    def get_vcpus_mem_from_instance_type(
        cls,
        instance_type: str,
    ) -> Tuple[Optional[float], Optional[float]]:
        return catalog.get_vcpus_mem_from_instance_type(instance_type,
                                                        clouds='ionet')

    @classmethod
    def get_zone_shell_cmd(cls) -> Optional[str]:
        return None

    def make_deploy_resources_variables(
        self,
        resources: 'resources_lib.Resources',
        cluster_name: 'resources_utils.ClusterName',
        region: 'clouds.Region',
        zones: Optional[List['clouds.Zone']],
        num_nodes: int,
        dryrun: bool = False,
        volume_mounts: Optional[List['volume_lib.VolumeMount']] = None,
    ) -> Dict[str, Any]:
        del dryrun, volume_mounts  # Unused.
        assert zones is None, 'IONet does not support zones.'
        resources = resources.assert_launchable()
        
        # Get accelerator information
        acc_dict = self.get_accelerators_from_instance_type(
            resources.instance_type)
        custom_resources = resources_utils.make_ray_custom_resources_str(
            acc_dict)

        # Map instance type to IONet hardware specifications
        hardware_id, gpus_per_vm = ionet_utils.instance_type_to_hardware_config(
            resources.instance_type)

        resources_vars: Dict[str, Any] = {
            'instance_type': resources.instance_type,
            'hardware_id': hardware_id,
            'gpus_per_vm': gpus_per_vm,
            'custom_resources': custom_resources,
            'region': region.name,
            'num_nodes': num_nodes,
            'cluster_name': cluster_name.name_on_cloud,
        }

        if acc_dict is not None:
            # IONet VMs include GPU access
            resources_vars['has_gpus'] = True

        return resources_vars

    def _get_feasible_launchable_resources(
        self, resources: 'resources_lib.Resources'
    ) -> 'resources_utils.FeasibleResources':
        """Filter and return feasible resources for IONet."""
        if resources.use_spot:
            return resources_utils.FeasibleResources([], [], [])

        if resources.instance_type is not None:
            # User specified instance type - validate it exists
            assert self.is_same_cloud(resources.cloud), resources
            instance_list = ([resources.copy(cloud=IONet())])
            return resources_utils.make_feasible_resources(instance_list)

        # Auto-select instance type based on requirements
        def _make(instance_type: str):
            r = resources.copy(
                cloud=IONet(),
                instance_type=instance_type,
                # IONet determines accelerators based on instance type
                accelerators=self.get_accelerators_from_instance_type(instance_type),
                cpus=None,
                memory=None,
            )
            return r

        # Get feasible instance types from catalog
        instance_types = catalog.get_instance_types_for_accelerators(
            resources.accelerators,
            resources.use_spot,
            region=resources.region,
            zone=resources.zone,
            clouds='ionet')

        instance_list = [_make(instance_type) for instance_type in instance_types]
        return resources_utils.make_feasible_resources(instance_list)

    @classmethod
    def check_credentials(
        cls, cloud_capability: clouds.CloudCapability
    ) -> Tuple[bool, Optional[Union[str, Dict[str, str]]]]:
        """Check IONet credentials."""
        return cls._check_credentials()

    @classmethod
    def get_current_user_identity(cls) -> Optional[List[str]]:
        # IONet uses API keys, no complex identity
        return None

    @classmethod 
    def get_user_identities(cls) -> Optional[List[List[str]]]:
        # IONet doesn't support multiple identities
        return None