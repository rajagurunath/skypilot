"""IONet utility functions and API client."""

import json
import os
import time
import typing
from typing import Any, Dict, List, Optional, Tuple

from sky.adaptors import common as adaptors_common
from sky import sky_logging
from sky.utils import common_utils

if typing.TYPE_CHECKING:
    import requests
else:
    requests = adaptors_common.LazyImport('requests')

logger = sky_logging.init_logger(__name__)

# IONet API configuration
API_KEY_PATH = '~/.ionet/api_key'
API_BASE_URL = 'https://api.intelligence.io.solutions'  # Replace with actual IONet API base URL
INITIAL_BACKOFF_SECONDS = 5
MAX_BACKOFF_FACTOR = 10
MAX_ATTEMPTS = 6

# Instance type mappings from SkyPilot to IONet
INSTANCE_TYPE_MAPPINGS = {
    # Format: 'skypilot-instance-type': (hardware_id, gpus_per_vm)
    'ionet-h100-1x': (1, 1),  # Example mapping - needs real data from IONet
    'ionet-h100-2x': (1, 2),
    'ionet-h100-4x': (1, 4),
    'ionet-h100-8x': (1, 8),
    'ionet-a100-1x': (2, 1),
    'ionet-a100-2x': (2, 2),
    'ionet-a100-4x': (2, 4),
    'ionet-a100-8x': (2, 8),
    'ionet-rtx4090-1x': (3, 1),
    'ionet-rtx4090-2x': (3, 2),
    'ionet-rtx4090-4x': (3, 4),
}

# Region mappings from SkyPilot to IONet location_ids
REGION_MAPPINGS = {
    'us-east-1': 1,
    'us-west-1': 2,
    'eu-west-1': 3,
    'ap-southeast-1': 4,
    # Add more mappings based on IONet's actual regions
}


class IONetError(Exception):
    """Exception for IONet API errors."""
    pass


class IONetMetadata:
    """Per-cluster metadata file management for IONet."""
    
    def __init__(self, cluster_name: str):
        self.cluster_name = cluster_name
        self.path = os.path.expanduser(f'~/.ionet/deployments-{cluster_name}.json')
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
    
    def set_deployment(self, deployment_id: str, deployment_info: Optional[Dict[str, Any]]) -> None:
        """Store or remove deployment metadata."""
        metadata = self._load_metadata()
        if deployment_info is None:
            # Remove deployment
            if deployment_id in metadata:
                del metadata[deployment_id]
                if not metadata:
                    # Remove file if empty
                    if os.path.exists(self.path):
                        os.remove(self.path)
                    return
        else:
            metadata[deployment_id] = deployment_info
        self._save_metadata(metadata)
    
    def get_deployment(self, deployment_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve deployment metadata."""
        metadata = self._load_metadata()
        return metadata.get(deployment_id)
    
    def list_deployments(self) -> Dict[str, Dict[str, Any]]:
        """List all deployments for this cluster."""
        return self._load_metadata()
    
    def refresh_deployments(self, active_deployment_ids: List[str]) -> None:
        """Clean up metadata for terminated deployments."""
        metadata = self._load_metadata()
        for deployment_id in list(metadata.keys()):
            if deployment_id not in active_deployment_ids:
                del metadata[deployment_id]
        if not metadata:
            if os.path.exists(self.path):
                os.remove(self.path)
        else:
            self._save_metadata(metadata)
    
    def _load_metadata(self) -> Dict[str, Any]:
        if not os.path.exists(self.path):
            return {}
        try:
            with open(self.path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f'Error loading metadata from {self.path}: {e}')
            return {}
    
    def _save_metadata(self, metadata: Dict[str, Any]) -> None:
        try:
            with open(self.path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2)
        except IOError as e:
            logger.error(f'Error saving metadata to {self.path}: {e}')


def get_api_key() -> str:
    """Get IONet API key from credentials file."""
    api_key_path = os.path.expanduser(API_KEY_PATH)
    if not os.path.exists(api_key_path):
        raise IONetError(f'IONet API key not found at {api_key_path}. '
                        'Please create the file and add your API key.')
    
    with open(api_key_path, 'r', encoding='utf-8') as f:
        api_key = f.read().strip()
    
    if not api_key:
        raise IONetError(f'IONet API key is empty in {api_key_path}')
    
    return api_key


def instance_type_to_hardware_config(instance_type: str) -> Tuple[int, int]:
    """Convert SkyPilot instance type to IONet hardware_id and gpus_per_vm."""
    if instance_type not in INSTANCE_TYPE_MAPPINGS:
        raise IONetError(f'Unsupported instance type: {instance_type}')
    
    return INSTANCE_TYPE_MAPPINGS[instance_type]


def region_to_location_id(region: str) -> int:
    """Convert SkyPilot region to IONet location_id."""
    if region not in REGION_MAPPINGS:
        raise IONetError(f'Unsupported region: {region}')
    
    return REGION_MAPPINGS[region]


def _make_request_with_backoff(method: str, url: str, headers: Dict[str, str],
                               data: Optional[str] = None, json_data: Optional[Dict] = None):
    """Make HTTP request with exponential backoff."""
    backoff = common_utils.Backoff(initial_backoff=INITIAL_BACKOFF_SECONDS,
                                   max_backoff_factor=MAX_BACKOFF_FACTOR)
    
    for attempt in range(MAX_ATTEMPTS):
        try:
            if method.upper() == 'GET':
                response = requests.get(url, headers=headers)
            elif method.upper() == 'POST':
                if json_data:
                    response = requests.post(url, headers=headers, json=json_data)
                else:
                    response = requests.post(url, headers=headers, data=data)
            elif method.upper() == 'DELETE':
                response = requests.delete(url, headers=headers)
            else:
                raise ValueError(f'Unsupported HTTP method: {method}')
            
            # Handle rate limiting
            if response.status_code == 429 and attempt < MAX_ATTEMPTS - 1:
                time.sleep(backoff.current_backoff())
                continue
            
            if response.status_code >= 400:
                error_msg = f'HTTP {response.status_code}: {response.reason}'
                try:
                    error_data = response.json()
                    if 'error' in error_data:
                        error_msg += f' - {error_data["error"]}'
                except:
                    error_msg += f' - {response.text}'
                raise IONetError(error_msg)
            
            return response
            
        except requests.RequestException as e:
            if attempt < MAX_ATTEMPTS - 1:
                logger.warning(f'Request failed (attempt {attempt + 1}): {e}')
                time.sleep(backoff.current_backoff())
                continue
            raise IONetError(f'Request failed after {MAX_ATTEMPTS} attempts: {e}')
    
    raise IONetError(f'Request failed after {MAX_ATTEMPTS} attempts')


class IONetClient:
    """IONet API client."""
    
    def __init__(self):
        self.api_key = get_api_key()
        self.headers = {
            'x-api-key': self.api_key,
            'Content-Type': 'application/json'
        }
    
    def test_connection(self):
        """Test if the API connection works by listing deployments."""
        try:
            # Use a real API call that requires authentication
            deployments = self.list_deployments()
            # If we can list deployments, credentials are valid
            return True
        except Exception as e:
            raise IONetError(f'Failed to authenticate with IONet API: {str(e)}')
    
    def deploy_vms(self, deploy_request: Dict[str, Any]) -> Dict[str, Any]:
        """Deploy VMs using IONet VMaaS API."""
        response = self._make_request('POST', '/enterprise/v1/io-cloud/vmaas/deploy',
                                    json_data=deploy_request)
        return response.json()
    
    def get_deployment_details(self, deployment_id: str) -> Dict[str, Any]:
        """Get deployment details."""
        response = self._make_request('GET', 
                                    f'/enterprise/v1/io-cloud/vmaas/deployment/{deployment_id}')
        return response.json()
    
    def get_deployment_vms(self, deployment_id: str) -> Dict[str, Any]:
        """Get VMs in a deployment."""
        response = self._make_request('GET',
                                    f'/enterprise/v1/io-cloud/vmaas/deployment/{deployment_id}/vms')
        return response.json()
    
    def destroy_deployment(self, deployment_id: str) -> Dict[str, Any]:
        """Destroy a deployment."""
        response = self._make_request('DELETE',
                                    f'/enterprise/v1/io-cloud/vmaas/deployment/{deployment_id}')
        return response.json() if response.text else {}
    
    def list_deployments(self) -> Dict[str, Any]:
        """List all deployments for the authenticated user."""
        response = self._make_request('GET', '/enterprise/v1/io-cloud/vmaas/deployments')
        return response.json()
    
    def get_available_hardware(self) -> Dict[str, Any]:
        """Get available hardware types."""
        response = self._make_request('GET',
                                    '/enterprise/v1/io-cloud/vmaas/hardware/max-gpus-per-vm')
        return response.json()
    
    def get_available_vms(self, hardware_id: int, hardware_qty: int, 
                          location_id: Optional[int] = None) -> Dict[str, Any]:
        """Check VM availability."""
        params = f'hardware_id={hardware_id}&hardware_qty={hardware_qty}'
        if location_id:
            params += f'&location_ids=[{location_id}]'
        
        response = self._make_request('GET',
                                    f'/enterprise/v1/io-cloud/vmaas/available-vms?{params}')
        return response.json()
    
    def get_pricing(self, hardware_id: int, location_ids: List[int], duration_hours: int,
                    gpus_per_vm: int, replica_count: int) -> Dict[str, Any]:
        """Get pricing information."""
        params = (f'hardware_id={hardware_id}&location_ids={json.dumps(location_ids)}'
                 f'&duration_hours={duration_hours}&gpus_per_vm={gpus_per_vm}'
                 f'&replica_count={replica_count}&currency=usdc')
        
        response = self._make_request('GET',
                                    f'/enterprise/v1/io-cloud/vmaas/price?{params}')
        return response.json()
    
    def _make_request(self, method: str, endpoint: str, json_data: Optional[Dict] = None):
        """Make API request to IONet."""
        url = API_BASE_URL + endpoint
        return _make_request_with_backoff(method, url, self.headers, json_data=json_data)


def extract_external_ip(ssh_access: str) -> str:
    """Extract external IP from IONet SSH access string."""
    # IONet provides SSH access in format: user@ip or ssh user@ip
    if '@' in ssh_access:
        return ssh_access.split('@')[-1].strip()
    return ssh_access.strip()


def extract_deployment_id(cluster_name_on_cloud: str) -> str:
    """Extract deployment ID from cluster name."""
    # For now, assume cluster_name_on_cloud contains or is the deployment_id
    # This may need adjustment based on actual naming patterns
    return cluster_name_on_cloud


def extract_cluster_name(cluster_name_on_cloud: str) -> str:
    """Extract original cluster name from cloud cluster name."""
    # Remove any prefixes/suffixes added by SkyPilot
    return cluster_name_on_cloud.replace('-head', '').replace('-worker', '')


def generate_ssh_keys() -> Dict[str, str]:
    """Generate or get existing SSH keys for IONet."""
    # This is a placeholder - implement actual SSH key management
    # For now, return empty dict (IONet can use GitHub IDs instead)
    return {}