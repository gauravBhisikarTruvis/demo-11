import json
import re
import requests
from fastapi import HTTPException
from src.utils.config_reader import load_config

# Load configuration
config = load_config()
section = 'TIGER_DB'
host = config.get(section, 'host')
username = config.get(section, 'username')
password = config.get(section, 'password')
graph = config.get(section, 'graph')

def execute_gsql(query: str):
    """
    Executes a GSQL query against the TigerGraph server.
    Returns a standardized dictionary with 'is_valid' and 'raw_response'.
    """
    TG_EXECUTE_URL = f"{host}/gsql/v1/statements"
    
    # Prepend 'USE GRAPH' to ensure the session knows which graph to use
    statement_body = f"USE GRAPH {graph} {query}"
    
    exec_headers = {
        "Content-Type": "text/plain",
        "Accept": "application/json",
    }

    try:
        # Execute the POST request
        exec_resp = requests.post(
            TG_EXECUTE_URL,
            auth=(username, password),
            headers=exec_headers,
            data=statement_body,
            timeout=60,
            verify=False  # Set to True if you have valid SSL certs
        )

        raw_text = exec_resp.text

        # CHECK 1: HTTP Status Code
        if exec_resp.status_code != 200:
            return {
                "is_valid": False,
                "execution_result": None,
                "raw_response": f"HTTP Error {exec_resp.status_code}: {raw_text}",
                "validation_error": f"Server returned status {exec_resp.status_code}"
            }

        # CHECK 2: Extract JSON from the response
        # TigerGraph sometimes returns text mixed with JSON. We look for the first JSON object.
        match = re.search(r"\{.*\}", raw_text, flags=re.DOTALL)
        
        if match:
            try:
                tg_result = json.loads(match.group(0))
                
                # Success path
                return {
                    "is_valid": True,
                    "execution_result": tg_result,
                    "raw_response": raw_text,
                    "validation_error": ""
                }
            except json.JSONDecodeError:
                # HTTP 200, but regex matched invalid JSON
                return {
                    "is_valid": False,
                    "execution_result": None,
                    "raw_response": raw_text,
                    "validation_error": "Response contained invalid JSON."
                }
        else:
            # HTTP 200, but no JSON found in text (likely just a status message string)
            return {
                "is_valid": False,  # Changed to False because you likely expect data
                "execution_result": None,
                "raw_response": raw_text,
                "validation_error": "No JSON object found in response."
            }

    except requests.exceptions.Timeout:
        return {
            "is_valid": False,
            "execution_result": None,
            "raw_response": "Request Timed Out",
            "validation_error": "Connection timed out (60s)."
        }
    except Exception as e:
        return {
            "is_valid": False,
            "execution_result": None,
            "raw_response": str(e),
            "validation_error": f"System Error: {str(e)}"
        }
