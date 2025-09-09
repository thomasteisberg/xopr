"""
This module provides functions to interact with part of the Open Polar Server (OPS) API
that do not require authentication.
It includes functions to retrieve segment IDs, layer points, and segment metadata.
"""

import base64
import requests
import json
import urllib.parse

ops_base_url = "https://ops.cresis.ku.edu/ops"

def get_layer_points(segment_name : str, season_name : str, location=None, layer_names=None, include_geometry=True):
    """
    Get layer points for a segment from the OPS API.

    Parameters
    ----------
    segment_name : str
        The segment name
    season_name : str
        The season name
    location : str, optional
        The location, either 'arctic' or 'antarctic'. If None, inferred from season_name.
    layer_names : list of str, optional
        List of layer names to retrieve. If None, retrieves all layers.
    include_geometry : bool, optional
        Whether to include geometry information in the response. Default is True.

    Returns
    -------
    dict or None
        API response as JSON containing layer points data, or None if request fails.
        
    Raises
    ------
    ValueError
        If neither segment_id nor both segment_name and season_name are provided.
    """

    url = f"{ops_base_url}/get/layer/points"

    if location is None:
        if "antarctica" in season_name.lower():
            location = "antarctic"
        elif "greenland" in season_name.lower():
            location = "arctic"
        else:
            raise ValueError("Location could not be inferred from the season name. Please specify 'arctic' or 'antarctic' explicitly.")

    data_payload = {
        "properties": {
            "location": location,
            "season": season_name,
            "segment": segment_name
        }
    }

    if include_geometry:
        data_payload["properties"]["return_geom"] = 'geog'
    
    # Add layer names if specified
    if layer_names:
        data_payload["properties"]["lyr_name"] = layer_names
    
    # URL encode the JSON data
    encoded_data = urllib.parse.quote(json.dumps(data_payload))
    
    # Form data
    form_data = f"app=rds&data={encoded_data}"
    
    # Create basic auth header for anonymous user
    credentials = base64.b64encode(b"anonymous:anonymous").decode("ascii")

    # Headers with basic authentication
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Authorization': f'Basic {credentials}',
        'Cookie': 'userName=anonymous; isAuthenticated=0'
    }
    
    try:
        # Make the POST request
        response = requests.post(url, data=form_data, headers=headers)
        
        # Check if request was successful
        response.raise_for_status()
        
        # Return JSON response
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"Error making request: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {e}")
        print(f"Response text: {response.text}")
        return None


def get_segment_metadata(segment_name : str, season_name : str):
    """
    Get segment metadata from the OPS API.

    Parameters
    ----------
    segment_name : str, optional
        The segment name (alternative to segment_id).
    season_name : str, optional
        The season name (required if using segment_name).

    Returns
    -------
    dict or None
        API response as JSON containing segment metadata, or None if request fails.
        
    Raises
    ------
    ValueError
        If neither segment_id nor both segment_name and season_name are provided.
    """

    url = f"{ops_base_url}/get/segment/metadata"

    data_payload = {
        "properties": {
            "segment": segment_name,
            "season": season_name
        }
    }
    
    # URL encode the JSON data
    encoded_data = urllib.parse.quote(json.dumps(data_payload))
    
    # Form data as sent in the original request
    form_data = f"app=rds&data={encoded_data}"
    
    # Minimal headers
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
    }
    
    try:
        # Make the POST request
        response = requests.post(url, data=form_data, headers=headers)
        
        # Check if request was successful
        response.raise_for_status()

        # Check if the response indicates a valid segment
        if response.json()['status'] == 0:
            print(f"Segment {segment_name} not found in season {season_name}. Error: {response.json().get('data', 'Unknown error')}")
            return None
        
        # Return JSON response
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"Error making request: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {e}")
        print(f"Response text: {response.text}")
        return None