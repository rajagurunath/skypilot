"""IONet instance provisioning."""

import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sky import sky_logging
from sky.provision import common
from sky.provision.ionet import utils as ionet_utils
from sky.utils import common_utils
from sky.utils import resources_utils
from sky.utils import status_lib
from sky.utils import ux_utils

logger = sky_logging.init_logger(__name__)

POLL_INTERVAL = 10  # seconds
MAX_POLL_ATTEMPTS = 180  # 30 minutes maximum

_ionet_client = None


def _get_ionet_client():
    """Get cached IONet client."""
    global _ionet_client
    if _ionet_client is None:
        _ionet_client = ionet_utils.IONetClient()
    return _ionet_client


def _wait_for_deployment_ready(deployment_id: str, timeout_minutes: int = 30) -> bool:
    """Wait for deployment to be ready."""
    client = _get_ionet_client()
    max_attempts = timeout_minutes * 60 // POLL_INTERVAL
    
    for attempt in range(max_attempts):
        try:
            deployment_details = client.get_deployment_details(deployment_id)
            status = deployment_details.get('data', {}).get('status', 'unknown')
            
            if status == 'running':
                return True
            elif status in ['failed', 'destroyed']:
                raise ionet_utils.IONetError(f'Deployment {deployment_id} failed with status: {status}')
            
            logger.info(f'Deployment {deployment_id} status: {status} (attempt {attempt + 1}/{max_attempts})')
            time.sleep(POLL_INTERVAL)
            
        except Exception as e:
            logger.warning(f'Error checking deployment status: {e}')
            time.sleep(POLL_INTERVAL)
    
    return False


def run_instances(region: str, cluster_name: str, cluster_name_on_cloud: str,
                  config: common.ProvisionConfig) -> common.ProvisionRecord:
    """Deploy VMs on IONet."""
    del cluster_name  # unused
    
    logger.info(f'Starting IONet deployment for cluster {cluster_name_on_cloud}')
    
    client = _get_ionet_client()
    
    # Check if cluster already exists
    metadata = ionet_utils.IONetMetadata(ionet_utils.extract_cluster_name(cluster_name_on_cloud))
    existing_deployments = metadata.list_deployments()
    
    if existing_deployments:
        # Check if existing deployment is still active
        for deployment_id, deployment_info in existing_deployments.items():
            try:
                deployment_details = client.get_deployment_details(deployment_id)
                status = deployment_details.get('data', {}).get('status')
                
                if status == 'running':
                    logger.info(f'Found existing running deployment: {deployment_id}')
                    return common.ProvisionRecord(
                        provider_name='ionet',
                        cluster_name=cluster_name_on_cloud,
                        region=region,
                        zone=None,
                        head_instance_id=deployment_id,
                        resumed_instance_ids=[],
                        created_instance_ids=[],
                    )
            except Exception as e:
                logger.warning(f'Error checking existing deployment {deployment_id}: {e}')
                # Clean up invalid deployment from metadata
                metadata.set_deployment(deployment_id, None)
    
    # Get hardware configuration from instance type
    try:
        hardware_id, gpus_per_vm = ionet_utils.instance_type_to_hardware_config(
            config.node_config.get('InstanceType', 'ionet-h100-1x'))
        location_id = ionet_utils.region_to_location_id(region)
    except ionet_utils.IONetError as e:
        raise RuntimeError(f'Invalid configuration: {e}')
    
    # Create deployment request
    deploy_request = {
        'resource_private_name': cluster_name_on_cloud,
        'duration_hours': config.node_config.get('duration_hours', 24),
        'gpus_per_vm': gpus_per_vm,
        'hardware_id': hardware_id,
        'location_ids': [location_id],
        'vms_qty': config.count,
        'vm_image_type': config.node_config.get('vm_image_type', 'general'),
        'ssh_keys': config.node_config.get('ssh_keys', {}),
        'github_ids': config.node_config.get('github_ids', []),
    }
    
    # Add network services if specified
    if 'network_services' in config.node_config:
        deploy_request['network_services'] = config.node_config['network_services']
    
    logger.info(f'Deploying IONet VMs with config: {deploy_request}')
    
    try:
        # Deploy VMs
        deployment_response = client.deploy_vms(deploy_request)
        deployment_id = deployment_response.get('deployment_id')
        
        if not deployment_id:
            raise ionet_utils.IONetError('No deployment_id returned from IONet API')
        
        logger.info(f'IONet deployment started: {deployment_id}')
        
        # Store deployment metadata
        deployment_info = {
            'cluster_name': ionet_utils.extract_cluster_name(cluster_name_on_cloud),
            'cluster_name_on_cloud': cluster_name_on_cloud,
            'deployment_id': deployment_id,
            'region': region,
            'hardware_id': hardware_id,
            'gpus_per_vm': gpus_per_vm,
            'vm_count': config.count,
            'vm_mappings': {},
            'created_at': datetime.utcnow().isoformat(),
            'deployment_status': 'deploying'
        }
        metadata.set_deployment(deployment_id, deployment_info)
        
        # Wait for deployment to be ready
        logger.info(f'Waiting for deployment {deployment_id} to be ready...')
        if not _wait_for_deployment_ready(deployment_id):
            raise ionet_utils.IONetError(f'Deployment {deployment_id} failed to become ready')
        
        logger.info(f'IONet deployment {deployment_id} is ready')
        
        return common.ProvisionRecord(
            provider_name='ionet',
            cluster_name=cluster_name_on_cloud,
            region=region,
            zone=None,
            head_instance_id=deployment_id,
            resumed_instance_ids=[],
            created_instance_ids=[deployment_id],
        )
        
    except Exception as e:
        logger.error(f'Failed to deploy IONet VMs: {e}')
        with ux_utils.print_exception_no_traceback():
            raise RuntimeError(f'IONet deployment failed: {common_utils.format_exception(e, use_bracket=False)}') from e


def wait_instances(region: str, cluster_name_on_cloud: str,
                   state: Optional[status_lib.ClusterStatus]) -> None:
    """Wait for instances to reach desired state."""
    del region, state  # Unused
    
    # For IONet, deployment creation already waits for readiness
    logger.debug(f'IONet instances for {cluster_name_on_cloud} are ready')


def stop_instances(
    cluster_name_on_cloud: str,
    provider_config: Optional[Dict[str, Any]] = None,
    worker_only: bool = False,
) -> None:
    """Stop instances (not supported by IONet)."""
    raise NotImplementedError('IONet does not support stopping instances. Use terminate instead.')


def terminate_instances(
    cluster_name_on_cloud: str,
    provider_config: Optional[Dict[str, Any]] = None,
    worker_only: bool = False,
) -> None:
    """Terminate IONet deployment."""
    del provider_config, worker_only  # unused for now (single deployment)
    
    client = _get_ionet_client()
    cluster_name = ionet_utils.extract_cluster_name(cluster_name_on_cloud)
    metadata = ionet_utils.IONetMetadata(cluster_name)
    
    deployments = metadata.list_deployments()
    
    if not deployments:
        logger.warning(f'No deployments found for cluster {cluster_name_on_cloud}')
        return
    
    for deployment_id in list(deployments.keys()):
        try:
            logger.info(f'Terminating IONet deployment: {deployment_id}')
            client.destroy_deployment(deployment_id)
            
            # Remove from metadata
            metadata.set_deployment(deployment_id, None)
            logger.info(f'Successfully terminated deployment: {deployment_id}')
            
        except Exception as e:
            logger.error(f'Failed to terminate deployment {deployment_id}: {e}')
            # Still remove from metadata to avoid stuck deployments
            metadata.set_deployment(deployment_id, None)


def get_cluster_info(
    region: str,
    cluster_name_on_cloud: str,
    provider_config: Optional[Dict[str, Any]] = None,
) -> common.ClusterInfo:
    """Get cluster information from IONet."""
    del region  # unused
    
    client = _get_ionet_client()
    cluster_name = ionet_utils.extract_cluster_name(cluster_name_on_cloud)
    metadata = ionet_utils.IONetMetadata(cluster_name)
    
    deployments = metadata.list_deployments()
    
    if not deployments:
        return common.ClusterInfo(
            instances={},
            head_instance_id=None,
            provider_name='ionet',
            provider_config=provider_config,
        )
    
    # For now, assume single deployment per cluster
    deployment_id = next(iter(deployments.keys()))
    deployment_info = deployments[deployment_id]
    
    try:
        # Get VMs from IONet API
        vms_response = client.get_deployment_vms(deployment_id)
        vms_data = vms_response.get('data', {})
        workers = vms_data.get('workers', [])
        
        if not workers:
            logger.warning(f'No VMs found for deployment {deployment_id}')
            return common.ClusterInfo(
                instances={},
                head_instance_id=None,
                provider_name='ionet',
                provider_config=provider_config,
            )
        
        instances = {}
        head_instance_id = None
        
        for i, vm in enumerate(workers):
            vm_id = vm.get('vm_id') or vm.get('container_id', f'vm-{i}')
            is_head = i == 0  # First VM is head
            
            # Extract networking information
            ssh_access = vm.get('ssh_access', '')
            external_ip = ionet_utils.extract_external_ip(ssh_access) if ssh_access else None
            internal_ip = vm.get('internal_ip', '127.0.0.1')
            
            # Create tags for SkyPilot
            vm_tags = {
                'ray-cluster-name': cluster_name_on_cloud,
                'ray-node-kind': 'head' if is_head else 'worker',
                'skypilot-cluster-name': cluster_name,
                'ionet-deployment-id': deployment_id,
            }
            
            instances[vm_id] = [
                common.InstanceInfo(
                    instance_id=vm_id,
                    internal_ip=internal_ip,
                    external_ip=external_ip,
                    ssh_port=22,
                    tags=vm_tags,
                )
            ]
            
            if is_head:
                head_instance_id = vm_id
            
            # Update local metadata with VM info
            if 'vm_mappings' not in deployment_info:
                deployment_info['vm_mappings'] = {}
            deployment_info['vm_mappings'][vm_id] = {
                'ray-node-kind': 'head' if is_head else 'worker',
                'internal_ip': internal_ip,
                'external_ip': external_ip,
                'ssh_access': ssh_access,
            }
        
        # Update metadata with VM mappings
        metadata.set_deployment(deployment_id, deployment_info)
        
        return common.ClusterInfo(
            instances=instances,
            head_instance_id=head_instance_id,
            provider_name='ionet',
            provider_config=provider_config,
        )
        
    except Exception as e:
        logger.error(f'Failed to get cluster info for {deployment_id}: {e}')
        return common.ClusterInfo(
            instances={},
            head_instance_id=None,
            provider_name='ionet',
            provider_config=provider_config,
        )


def query_instances(
    cluster_name: str,
    cluster_name_on_cloud: str,
    provider_config: Optional[Dict[str, Any]] = None,
    non_terminated_only: bool = True,
) -> Dict[str, Tuple[Optional['status_lib.ClusterStatus'], Optional[str]]]:
    """Query IONet deployment status."""
    del cluster_name  # unused
    
    client = _get_ionet_client()
    metadata = ionet_utils.IONetMetadata(ionet_utils.extract_cluster_name(cluster_name_on_cloud))
    
    deployments = metadata.list_deployments()
    statuses: Dict[str, Tuple[Optional['status_lib.ClusterStatus'], Optional[str]]] = {}
    
    for deployment_id in deployments:
        try:
            deployment_details = client.get_deployment_details(deployment_id)
            ionet_status = deployment_details.get('data', {}).get('status', 'unknown')
            
            # Map IONet status to SkyPilot status
            status_map = {
                'deployment requested': status_lib.ClusterStatus.INIT,
                'running': status_lib.ClusterStatus.UP,
                'failed': status_lib.ClusterStatus.INIT,  # Could be transient
                'completed': None,
                'destroyed': None,
                'termination requested': None,
            }
            
            skypilot_status = status_map.get(ionet_status, status_lib.ClusterStatus.INIT)
            
            if non_terminated_only and skypilot_status is None:
                continue
                
            statuses[deployment_id] = (skypilot_status, None)
            
        except Exception as e:
            logger.warning(f'Failed to query deployment {deployment_id}: {e}')
            if not non_terminated_only:
                statuses[deployment_id] = (None, str(e))
    
    return statuses


def cleanup_ports(
    cluster_name_on_cloud: str,
    ports: List[str],
    provider_config: Optional[Dict[str, Any]] = None,
) -> None:
    """Clean up firewall rules (IONet manages this at deployment level)."""
    del cluster_name_on_cloud, ports, provider_config  # unused
    logger.info('IONet manages network services at deployment level - no port cleanup needed')


def open_ports(
    cluster_name_on_cloud: str,
    ports: List[str],
    provider_config: Optional[Dict[str, Any]] = None,
) -> None:
    """Open firewall ports (IONet manages this at deployment level)."""
    del cluster_name_on_cloud, ports, provider_config  # unused
    logger.info('IONet manages network services at deployment level - ports configured during deployment')