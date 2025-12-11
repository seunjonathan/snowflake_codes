import requests
import json
import time
import yaml
import argparse
import sys
import os
from pathlib import Path

# --- GLOBAL CONFIGURATION ---
NIFI_HOST = None
AUTH_HEADERS = None
VERIFY_SSL = True
# --- END CONFIGURATION ---

def load_config(yaml_file):
    """Load configuration from YAML file."""
    try:
        with open(yaml_file, 'r') as f:
            config = yaml.safe_load(f)
        
        # Get nifi_host from YAML or environment variable
        if 'nifi_host' not in config or not config['nifi_host']:
            config['nifi_host'] = os.getenv('SNOWFLAKE_NIFI_HOST')
            if not config['nifi_host']:
                print("Error: 'nifi_host' must be provided in YAML or via SNOWFLAKE_NIFI_HOST environment variable.")
                sys.exit(1)
            else:
                print(f"Using nifi_host from SNOWFLAKE_NIFI_HOST environment variable")
        
        # Get bearer_token from YAML or environment variable
        if 'bearer_token' not in config or not config['bearer_token']:
            config['bearer_token'] = os.getenv('SNOWFLAKE_NIFI_TOKEN')
            if not config['bearer_token']:
                print("Error: 'bearer_token' must be provided in YAML or via SNOWFLAKE_NIFI_TOKEN environment variable.")
                sys.exit(1)
            else:
                print(f"Using bearer_token from SNOWFLAKE_NIFI_TOKEN environment variable")
        
        if 'parameter_contexts' not in config or not config['parameter_contexts']:
            print("Error: 'parameter_contexts' list is required and cannot be empty.")
            sys.exit(1)
        
        return config
    except FileNotFoundError:
        print(f"Error: Configuration file '{yaml_file}' not found.")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"Error parsing YAML file: {e}")
        sys.exit(1)

def find_parameter_context_by_name(pattern):
    """Find parameter context ID by name pattern."""
    print(f"Searching for Parameter Context matching pattern: '{pattern}'...")
    try:
        url = f"{NIFI_HOST}/nifi-api/flow/parameter-contexts"
        response = requests.get(url, headers=AUTH_HEADERS, verify=VERIFY_SSL)
        response.raise_for_status()
        
        contexts = response.json()['parameterContexts']
        pattern_lower = pattern.lower()
        
        # Find contexts that match the pattern
        matching_contexts = []
        for ctx in contexts:
            name = ctx['component']['name']
            if pattern_lower in name.lower():
                matching_contexts.append({
                    'id': ctx['id'],
                    'name': name
                })
        
        if not matching_contexts:
            print(f"Error: No parameter context found matching pattern '{pattern}'")
            exit(1)
        
        if len(matching_contexts) > 1:
            print(f"Found {len(matching_contexts)} matching contexts:")
            for i, ctx in enumerate(matching_contexts, 1):
                print(f"  {i}. {ctx['name']} (ID: {ctx['id']})")
            print("\nUsing the first match. To select a different one, update CONTEXT_NAME_PATTERN.")
        
        selected = matching_contexts[0]
        print(f"   -> Selected: '{selected['name']}' (ID: {selected['id']})")
        return selected['id']
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching parameter contexts: {e}")
        exit(1)

def get_current_context(context_uuid):
    """Step 1: Get the current context, revision, and parameters."""
    print("1. Fetching current Parameter Context data...")
    context_url = f"{NIFI_HOST}/nifi-api/parameter-contexts/{context_uuid}"
    try:
        response = requests.get(context_url, headers=AUTH_HEADERS, verify=VERIFY_SSL)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching context: {e}")
        exit(1)

def prepare_update_payload(context_data, parameter_updates):
    """Step 2: Prepare the JSON payload for the update request."""
    print("2. Preparing update payload...")
    
    # Get the required revision and component
    revision = context_data['revision']
    component = context_data['component']
    parameters = component['parameters']
    
    # Update multiple parameter values
    updated_count = 0
    not_found = []
    
    for param_update in parameter_updates:
        param_name = param_update['name']
        param_value = param_update['value']
        
        found = False
        for param_entry in parameters:
            if param_entry['parameter']['name'] == param_name:
                param_entry['parameter']['value'] = param_value
                found = True
                updated_count += 1
                print(f"   -> Updated parameter '{param_name}' to '{param_value}'")
                break
        
        if not found:
            not_found.append(param_name)
    
    if not_found:
        print(f"Warning: The following parameters were not found in the context: {', '.join(not_found)}")
    
    if updated_count == 0:
        print("Error: No parameters were updated.")
        exit(1)
    
    print(f"   -> Total parameters updated: {updated_count}")

    # Construct the final request payload - include the full component
    payload = {
        "revision": revision,
        "component": component
    }
    return payload

def post_update_request(payload, context_uuid):
    """Step 3: Post the update request to initiate the change."""
    print("3. Posting update request...")
    context_url = f"{NIFI_HOST}/nifi-api/parameter-contexts/{context_uuid}"
    update_request_url = f"{context_url}/update-requests"
    try:
        #print(f"   -> Payload: {json.dumps(payload, indent=2)}")
        response = requests.post(update_request_url, headers={**AUTH_HEADERS, "Content-Type": "application/json"}, 
                                 json=payload, verify=VERIFY_SSL)
        response.raise_for_status()
        return response.json()['request']
    except requests.exceptions.RequestException as e:
        print(f"Error posting update request: {e}")
        exit(1)

def monitor_request(request_data):
    """Step 4: Monitor the asynchronous update process until complete."""
    request_id = request_data['requestId']
    monitor_url = request_data['uri']
    
    print(f"4. Monitoring update request (ID: {request_id})...")
    
    while True:
        try:
            response = requests.get(monitor_url, headers=AUTH_HEADERS, verify=VERIFY_SSL)
            response.raise_for_status()
            status = response.json()['request']
            
            percent = status.get('percentCompleted', 0)
            state = status.get('state', 'Unknown')
            
            print(f"   -> Progress: {percent}% ({state})", end='\r')
            
            if status['complete']:
                if 'failureReason' in status:
                    print(f"\n❌ Update **Failed**! Reason: {status['failureReason']}")
                else:
                    print("\n✅ Update **Succeeded**! Parameter Context has been updated.")
                return status

            time.sleep(5) # Wait 5 seconds before checking again
        
        except requests.exceptions.RequestException as e:
            print(f"\nError monitoring request: {e}")
            exit(1)
            
def delete_request(request_data, context_uuid):
    """Step 5: Delete the request to clean up NiFi resources."""
    request_id = request_data['requestId']
    context_url = f"{NIFI_HOST}/nifi-api/parameter-contexts/{context_uuid}"
    delete_url = f"{context_url}/update-requests/{request_id}"
    
    print("5. Cleaning up update request...")
    try:
        response = requests.delete(delete_url, headers=AUTH_HEADERS, verify=VERIFY_SSL)
        response.raise_for_status()
        print("   -> Cleanup complete.")
    except requests.exceptions.RequestException as e:
        print(f"Error deleting request: {e}")


def process_parameter_context(context_config):
    """Process a single parameter context update."""
    context_name_pattern = context_config['context_name_pattern']
    parameter_updates = context_config['parameters']
    
    print(f"\n{'='*80}")
    print(f"Processing Parameter Context: {context_name_pattern}")
    print(f"{'='*80}")
    
    try:
        # 0. Find the parameter context by name pattern
        context_uuid = find_parameter_context_by_name(context_name_pattern)
        
        # 1. Get current data
        context_data = get_current_context(context_uuid)
        
        # 2. Prepare the payload with the new values
        update_payload = prepare_update_payload(context_data, parameter_updates)
        
        # 3. Start the asynchronous update
        update_request_data = post_update_request(update_payload, context_uuid)
        
        # 4. Monitor the request until complete
        final_status = monitor_request(update_request_data)
        
        # 5. Clean up the request
        delete_request(final_status, context_uuid)
        
        return True
    except Exception as e:
        print(f"\n❌ Failed to process context '{context_name_pattern}': {e}")
        return False

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Update NiFi Parameter Contexts from YAML configuration',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example YAML configuration:
---
nifi_host: "https://nifi.example.com"
bearer_token: "your-bearer-token-here"
verify_ssl: true

parameter_contexts:
  - context_name_pattern: "SQLServer Destination"
    parameters:
      - name: "SQLServer Username"
        value: "new_username"
      - name: "SQLServer Password"
        value: "new_password"
  
  - context_name_pattern: "SQLServer Source"
    parameters:
      - name: "Connection String"
        value: "Server=localhost;Database=mydb"
        """
    )
    parser.add_argument('--config_file', help='Path to YAML configuration file', default='nifi_config.yaml')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Validate configuration without making changes')
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config_file)
    
    # Set global configuration
    NIFI_HOST = config['nifi_host']
    AUTH_HEADERS = {"Authorization": f"Bearer {config['bearer_token']}"}
    VERIFY_SSL = config.get('verify_ssl', True)
    
    if args.dry_run:
        print("DRY RUN MODE - No changes will be made")
        print(f"\nNiFi Host: {NIFI_HOST}")
        print(f"SSL Verification: {VERIFY_SSL}")
        print(f"\nParameter Contexts to update: {len(config['parameter_contexts'])}")
        for ctx in config['parameter_contexts']:
            print(f"\n  - {ctx['context_name_pattern']}")
            for param in ctx['parameters']:
                print(f"      {param['name']}: {param['value']}")
        sys.exit(0)
    
    # Process each parameter context
    print(f"\nStarting batch update of {len(config['parameter_contexts'])} parameter context(s)...\n")
    
    results = []
    for context_config in config['parameter_contexts']:
        success = process_parameter_context(context_config)
        results.append({
            'context': context_config['context_name_pattern'],
            'success': success
        })
    
    # Print summary
    print(f"\n\n{'='*80}")
    print("BATCH UPDATE SUMMARY")
    print(f"{'='*80}")
    
    successful = sum(1 for r in results if r['success'])
    failed = len(results) - successful
    
    print(f"\nTotal contexts processed: {len(results)}")
    print(f"✅ Successful: {successful}")
    print(f"❌ Failed: {failed}")
    
    if failed > 0:
        print("\nFailed contexts:")
        for r in results:
            if not r['success']:
                print(f"  - {r['context']}")
        sys.exit(1)
    else:
        print("\n🎉 All parameter contexts updated successfully!")



# curl -v -X GET https://of--a4446422697571-cathaybank.snowflakecomputing.app/dataingestion/nifi-api/parameter-contexts/8c512dc0-fbfe-3a04-bd5e-9262de0c9cc9 -H "Authorization: Bearer eyJraWQiOiI5NjEyOTA5OTAzMzM5NTgiLCJhbGciOiJFUzI1NiJ9.eyJwIjoiMTQ2NjgxMzczNzM6MTQ2NjgxMzkzOTciLCJpc3MiOiJTRjoyMDA2IiwiZXhwIjoxNzkzMjYyNDI0fQ.SMPCMjie56gOoO1lP971N2-o2Ril3peRSKUrjhaxrZmx6cmelUAfpw74daRyAvi7qmVd8K67c3aJa0DBh0h4Tw"

# https://of--a4446422697571-cathaybank.snowflakecomputing.app/dataingestion/nifi/#/parameter-contexts/8c512dc0-fbfe-3a04-bd5e-9262de0c9cc9/edit