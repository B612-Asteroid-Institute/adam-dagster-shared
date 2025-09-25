import base64
import logging
import os
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Optional

import requests
from kubernetes import client as k8s_client
from kubernetes.client.rest import ApiException
from pydantic import BaseModel, Field, field_validator

from adam_dagster_shared.k8s import get_current_namespace, get_node_sa_kubernetes_client


class JobUpdateWebhook(BaseModel):
    """Request model for Dagster job update webhook."""

    job_id: str = Field(..., description="External job ID (Dagster run ID)")
    started_at: Optional[datetime] = Field(
        None, description="ISO timestamp when job execution began"
    )
    completed_at: Optional[datetime] = Field(
        None, description="ISO timestamp when job execution finished"
    )
    execution_duration_hours: Optional[float] = Field(
        None, ge=0, description="Total execution time in hours"
    )
    compute_cost_usd: Optional[Decimal] = Field(
        None, ge=0, description="Compute cost in USD"
    )
    peak_memory_gb: Optional[float] = Field(
        None, ge=0, description="Peak memory usage in GB"
    )
    cpu_hours: Optional[float] = Field(None, ge=0, description="CPU hours used")
    status: str = Field(..., description="Job status - must match Job.Status choices")
    request_data: Optional[Dict[str, Any]] = Field(
        None, description="Job request parameters and input data"
    )
    results: Optional[Dict[str, Any]] = Field(
        None, description="Job results and output data"
    )
    error_message: Optional[str] = Field(
        None, description="Error message if job failed"
    )

    @field_validator("status")
    @classmethod
    def validate_status(cls, v):
        """Validate that status matches Job.Status choices."""
        # Valid statuses based on Django Job.Status.choices (lowercase)
        valid_statuses = [
            "pending",
            "queued", 
            "starting",
            "running",
            "success",
            "failed",
            "canceled",
        ]
        if v not in valid_statuses:
            raise ValueError(f"Invalid status. Must be one of: {valid_statuses}")
        return v



class ADAMAPIClient:
    """Client for communicating with the ADAM API server."""
    
    def __init__(self, host: str, port: int, refresh_token: Optional[str] = None):
        """Initialize the API client with host, port, and optional refresh token.
        
        Args:
            host: The API server hostname or IP address
            port: The API server port number
            refresh_token: Refresh token for automatic access token management
        """
        self.host = host
        self.port = port
        self.refresh_token = refresh_token
        self._access_token: Optional[str] = (
            None  # Hidden state for current access token
        )
        self.base_url = f"http://{host}:{port}"
        
        # Set up session with default headers
        self.session = requests.Session()
        self.session.headers.update(
            {"Content-Type": "application/json", "Accept": "application/json"}
        )

    def _ensure_access_token(self) -> bool:
        """Ensure we have a valid access token, refreshing if necessary.

        Returns:
            bool: True if access token is available or no refresh token (anonymous), False otherwise
        """
        # If no refresh token is provided, allow anonymous requests
        if not self.refresh_token:
            return True

        # If refresh token is provided, ensure we have an access token
        if self._access_token is None:
            return self._refresh_access_token()

        return True

    def _refresh_access_token(self) -> bool:
        """Refresh the access token using the refresh token.

        Returns:
            bool: True if refresh successful, False otherwise
        """
        if not self.refresh_token:
            logging.warning("No refresh token available for access token refresh")
            return False

        try:
            # Temporarily remove any existing Authorization header
            original_auth = self.session.headers.pop("Authorization", None)

            # Make request to get new access token
            response = self.session.request(
                method="POST",
                url=f"{self.base_url.rstrip('/')}/api/oauth/access-tokens",
                json={"refresh_token": self.refresh_token},
            )

            if response.status_code == 200:
                response_data = response.json()
                access_token = response_data.get("access_token")
                if access_token:
                    self._access_token = access_token
                    self.session.headers.update(
                        {"Authorization": f"Bearer {access_token}"}
                    )
                    logging.info("Successfully refreshed access token")
                    return True

            # Restore original auth header if refresh failed
            if original_auth:
                self.session.headers["Authorization"] = original_auth

            logging.error(
                f"Failed to refresh access token: {response.status_code} {response.text}"
            )
            return False

        except Exception as e:
            logging.error(f"Exception during access token refresh: {e}")
            return False

    def _is_token_expired_response(self, response: requests.Response) -> bool:
        """Check if a response indicates an expired access token.

        Args:
            response: The HTTP response to check

        Returns:
            bool: True if the response indicates token expiration
        """
        if response.status_code != 401:
            return False

        # Try to check for specific token expiration indicators in the response
        try:
            error_data = response.json()
            
            # Check multiple common error fields for token expiration indicators
            fields_to_check = [
                error_data.get("error", "").lower(),
                error_data.get("detail", "").lower(), 
                error_data.get("error_description", "").lower(),
                error_data.get("error_code", "").lower(),
            ]
            
            # Check for various token expiration indicators
            expiration_indicators = ["token_expired", "expired", "invalid_token", "authentication_error"]
            is_expired = any(indicator in field for field in fields_to_check for indicator in expiration_indicators)
            
            # Debug logging to help diagnose token expiration detection
            logging.debug(f"Token expiration check - fields: {fields_to_check}, indicators found: {is_expired}")
            return is_expired
        except (ValueError, KeyError, AttributeError):
            # If we can't parse the JSON or get error details, assume any 401 is token expiration
            # This makes us more robust against API response format changes
            return True

    def _execute_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Execute a single HTTP request.

        Args:
            method: HTTP method
            url: Full URL to request
            **kwargs: Additional arguments for the request

        Returns:
            HTTP response
        """
        return self.session.request(method=method, url=url, **kwargs)

    def _handle_expired_token(
        self, response: requests.Response, method: str, url: str, **request_kwargs
    ) -> requests.Response:
        """Handle expired token responses by refreshing and retrying the request.

        Args:
            response: The original response to check
            method: HTTP method for retry
            url: Full URL for retry
            **request_kwargs: Additional arguments for retry request

        Returns:
            Either the original response (if not expired) or the retry response
        """
        # Add debug logging for diagnosis
        is_expired = self._is_token_expired_response(response)
        has_refresh_token = bool(self.refresh_token)
        logging.info(f"Token expiration check - expired: {is_expired}, has_refresh_token: {has_refresh_token}, status: {response.status_code}")
        
        # Pass through if not a token expiration or no refresh token available
        if not is_expired or not has_refresh_token:
            if not is_expired:
                logging.info("Response not identified as token expiration - skipping retry")
            if not has_refresh_token:
                logging.warning("No refresh token available - cannot attempt token refresh")
            return response

        logging.info("Access token expired, attempting to refresh and retry request")

        # Clear current token and refresh
        self._access_token = None
        if self._refresh_access_token():
            retry_response = self._execute_request(method, url, **request_kwargs)
            logging.info(f"Successfully retried request with refreshed token - new status: {retry_response.status_code}")
            return retry_response
        else:
            logging.error("Failed to refresh token after 401 response")
            return response  # Return original response if refresh failed

    def _make_request(
        self,
        endpoint: str,
        method: str = "GET",
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> requests.Response:
        """Make an HTTP request to the API server with automatic token refresh.
        
        Args:
            endpoint: API endpoint path (e.g., "/api/v1/jobs")
            method: HTTP method (GET, POST, PUT, DELETE)
            data: Request data for POST/PUT requests
            params: Query parameters for GET requests
            
        Returns:
            HTTP response
            
        Raises:
            requests.RequestException: If the request fails
        """
        # Ensure we have a valid access token before making the request
        if not self._ensure_access_token():
            logging.warning("No valid access token available for API request")

        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        request_kwargs = {"json": data, "params": params}

        # Make request and handle any token expiration
        response = self._execute_request(method, url, **request_kwargs)
        response = self._handle_expired_token(response, method, url, **request_kwargs)
        return response

    def get(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """Make a GET request.
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
            
        Returns:
            JSON response as a dictionary
        """
        return self._make_request(endpoint, "GET", params=params)
    
    def post(
        self, endpoint: str, data: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """Make a POST request.
        
        Args:
            endpoint: API endpoint path
            data: Request body data
            
        Returns:
            JSON response as a dictionary
        """
        return self._make_request(endpoint, "POST", data=data)
    
    def put(
        self, endpoint: str, data: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """Make a PUT request.
        
        Args:
            endpoint: API endpoint path
            data: Request body data
            
        Returns:
            JSON response as a dictionary
        """
        return self._make_request(endpoint, "PUT", data=data)
    
    def delete(self, endpoint: str) -> requests.Response:
        """Make a DELETE request.
        
        Args:
            endpoint: API endpoint path
            
        Returns:
            JSON response as a dictionary
        """
        return self._make_request(endpoint, "DELETE")

    def authenticate_with_refresh_token(self, refresh_token: str) -> bool:
        """Authenticate using a refresh token to get an access token.

        Args:
            refresh_token: The refresh token to exchange for an access token

        Returns:
            bool: True if authentication successful, False otherwise
        """
        self.refresh_token = refresh_token
        return self._refresh_access_token()

    def send_job_update(self, job_update_data: Dict[str, Any]) -> bool:
        """Send a job update to the ADAM API.

        Args:
            job_update_data: Dictionary containing job update information

        Returns:
            bool: True if update sent successfully, False otherwise
        """
        # Use the standalone function for consistency
        return send_job_update(self, **job_update_data)


def initialize_adam_api_client(
    host: str | None = None, port: int | None = None, refresh_token: str | None = None
) -> ADAMAPIClient:
    """Initialize the ADAM API client using Kubernetes service DNS with namespace.

    Host is constructed using the standard Kubernetes DNS format:
    <service>.<namespace>.svc.cluster.local

    - Service name defaults to "adam-api" and can be overridden via ADAM_API_SERVICE_NAME
    - Port is taken from the provided argument, or ADAM_API_SERVICE_PORT, or defaults to 80

    Returns an unauthenticated client. For authenticated access in Kubernetes deployments,
    use create_authenticated_adam_api_client() instead.

    Returns:
        AdamApiClient: Configured API client instance (unauthenticated)
    """
    if host is None:
        try:
            namespace = get_current_namespace()
        except Exception:
            namespace = os.getenv("POD_NAMESPACE", "default")

        service_name = os.getenv("ADAM_API_SERVICE_NAME", "adam-api")
        host = f"{service_name}.{namespace}.svc.cluster.local"

    final_port: int
    if port is not None:
        final_port = port
    else:
        port_env = os.getenv("ADAM_API_SERVICE_PORT")
        if port_env is not None:
            try:
                final_port = int(port_env)
            except ValueError:
                raise ValueError(
                    f"ADAM_API_SERVICE_PORT must be a valid integer, got: {port_env}"
                )
        else:
            final_port = 80

    # Return unauthenticated client - authentication should be done separately
    return ADAMAPIClient(host, final_port, refresh_token=refresh_token)


def create_authenticated_adam_api_client() -> ADAMAPIClient:
    """Create an authenticated ADAM API client using refresh token from Kubernetes secret.

    Reads the refresh token from the 'dagster-adam-api-token' secret in the current namespace
    and authenticates with the ADAM API using the Kubernetes API client.

    Returns:
        AdamApiClient: Authenticated API client
        
    Raises:
        ValueError: If required environment variables are missing
        Exception: If cluster connection or secret reading fails
    """
    # Get current Kubernetes context
    namespace = get_current_namespace()
    logging.info(f"Current Kubernetes namespace: {namespace}")

    # Get Kubernetes API client and read secret
    cluster = "adam-prod" if namespace == "production" else "adam-dev"
    api_client = get_node_sa_kubernetes_client("moeyens-thor-dev", "us-west1-a", cluster)
    v1 = k8s_client.CoreV1Api(api_client)
    
    secret = v1.read_namespaced_secret(name="dagster-adam-api-token", namespace=namespace)

    # Decode the refresh token
    secret_key = "refresh-token"
    if secret_key not in secret.data:
        secret_key = "token"
        if secret_key not in secret.data:
            available_keys = list(secret.data.keys()) if secret.data else []
            raise KeyError(
                f"Secret 'dagster-adam-api-token' does not contain key 'refresh-token' or 'token'. "
                f"Available keys: {available_keys}"
            )

    encoded_token = secret.data[secret_key]
    refresh_token = base64.b64decode(encoded_token).decode("utf-8")

    # Initialize and return API client with the refresh token
    client = initialize_adam_api_client(refresh_token=refresh_token)
    logging.info("Successfully initialized ADAM API client with refresh token")
    return client


# Global ADAM API client instance
_adam_api_client: Optional[ADAMAPIClient] = None


def get_adam_client() -> Optional[ADAMAPIClient]:
    """Get or create the global ADAM API client instance."""
    global _adam_api_client
    if _adam_api_client is None:
        try:
            logging.info("Creating authenticated ADAM API client...")
            _adam_api_client = create_authenticated_adam_api_client()
            logging.info("Successfully created authenticated ADAM API client")
        except Exception as e:
            logging.error(f"Failed to create authenticated ADAM API client: {e}")
            return None
    return _adam_api_client




def send_job_update(
    job_id: str,
    status: str,
    started_at: Optional[datetime] = None,
    completed_at: Optional[datetime] = None,
    execution_duration_hours: Optional[float] = None,
    compute_cost_usd: Optional[Decimal] = None,
    peak_memory_gb: Optional[float] = None,
    cpu_hours: Optional[float] = None,
    request_data: Optional[Dict[str, Any]] = None,
    results: Optional[Dict[str, Any]] = None,
    error_message: Optional[str] = None,
) -> bool:
    """Send a job update to the ADAM API with simple token refresh retry logic."""
    try:
        api_client = get_adam_client()
        if not api_client:
            logging.error("Failed to get API client")
            return False

        # Build payload - only include fields with actual values
        payload = {
            "job_id": job_id,
            "status": status
        }
        
        if started_at:
            payload["started_at"] = started_at
        if completed_at:
            payload["completed_at"] = completed_at
        if execution_duration_hours:
            payload["execution_duration_hours"] = execution_duration_hours
        if compute_cost_usd:
            payload["compute_cost_usd"] = compute_cost_usd
        if peak_memory_gb:
            payload["peak_memory_gb"] = peak_memory_gb
        if cpu_hours:
            payload["cpu_hours"] = cpu_hours
        if request_data:
            payload["request_data"] = request_data
        if results:
            payload["results"] = results
        if error_message:
            payload["error_message"] = error_message

        # Validate with Pydantic
        job_update_data = JobUpdateWebhook(**payload).model_dump(exclude_none=True, mode="json")
        
        # Prepare request
        url = f"{api_client.base_url}/api/internal/job-update"
        headers = {"Content-Type": "application/json"}
        
        # Get initial token if needed
        if api_client.refresh_token and not api_client._access_token:
            api_client._refresh_access_token()
        if api_client._access_token:
            headers["Authorization"] = f"Bearer {api_client._access_token}"

        # Make request
        response = requests.post(url, json=job_update_data, headers=headers)
        
        # Retry on 401 (Unauthorized) or 400 (Bad Request with auth errors)
        if response.status_code in [400, 401] and api_client.refresh_token:
            logging.info(f"Token expired for job {job_id}, refreshing and retrying...")
            api_client._access_token = None
            if api_client._refresh_access_token():
                headers["Authorization"] = f"Bearer {api_client._access_token}"
                response = requests.post(url, json=job_update_data, headers=headers)
        
        success = response.status_code == 200
        if success:
            logging.info(f"Successfully sent job update for {job_id}: {status}")
        else:
            logging.error(f"Failed job update for {job_id}: {response.status_code} {response.text}")
        return success

    except Exception as e:
        logging.exception(f"Failed to send job update: {e}")
        return False 