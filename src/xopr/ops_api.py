import requests
import json
import urllib.parse

def get_segment_id_by_name(segment_name, season_name):
    """
    Get segment ID by segment name and season
    
    Args:
        segment_name (str): The segment name (e.g., "20240105_02")
        season_name (str): The season name
    
    Returns:
        dict: API response, or None if not found
    """
    
    url = "https://ops.cresis.ku.edu/ops/get/segment/metadata"
    
    # Prepare the data payload using segment name and season
    data_payload = {
        "properties": {
            "segment": segment_name,
            "season": season_name
        }
    }
    
    # URL encode the JSON data
    encoded_data = urllib.parse.quote(json.dumps(data_payload))
    
    # Form data
    form_data = f"app=rds&data={encoded_data}"
    
    # Minimal headers
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
    }
    
    try:
        response = requests.post(url, data=form_data, headers=headers)
        response.raise_for_status()
        
        result = response.json()
        if result.get('status') == 1 and 'data' in result:
            print(f"Found segment: {segment_name} in season: {season_name}")
            return result
        else:
            print(f"Segment {segment_name} not found in season {season_name}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Error making request: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {e}")
        return None


def get_segment_metadata(segment_id=None, segment_name=None, season_name=None):
    """
    Get segment metadata from the OPS API
    
    Args:
        segment_id (int, optional): The segment ID to query
        segment_name (str, optional): The segment name (alternative to segment_id)
        season_name (str, optional): The season name (required if using segment_name)
    
    Returns:
        dict: API response as JSON
    """
    
    url = "https://ops.cresis.ku.edu/ops/get/segment/metadata"
    
    # Prepare the data payload - support both segment_id and segment_name approaches
    if segment_id is not None:
        data_payload = {
            "properties": {
                "segment_id": segment_id
            }
        }
    elif segment_name is not None and season_name is not None:
        data_payload = {
            "properties": {
                "segment": segment_name,
                "season": season_name
            }
        }
    else:
        raise ValueError("Must provide either segment_id or both segment_name and season_name")
    
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
        
        # Return JSON response
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"Error making request: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {e}")
        print(f"Response text: {response.text}")
        return None