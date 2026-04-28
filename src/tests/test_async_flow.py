#!/usr/bin/env python3
import requests
import time
import json

BASE_URL = "http://localhost:8000/api"

# Step 1: Get token
print("Step 1: Getting JWT token...")
response = requests.get(
    f"{BASE_URL}/auth/token",
    headers={"Content-Type": "application/json", "system": "silux_protecta"},
    json={"apikey": "dBGj6XLWfyK0xkScUpJTJjRx8z4vZ4bB"}
)
print(f"Token response: {response.status_code}")
token_data = response.json()
print(f"Token data: {token_data}")

if "access_token" not in token_data:
    print("ERROR: Failed to get token")
    exit(1)

access_token = token_data["access_token"]
headers = {
    "Content-Type": "application/json",
    "system": "silux_protecta",
    "Authorization": f"Bearer {access_token}"
}

# Step 2: Submit validation task
print("\nStep 2: Submitting validation task...")
response = requests.post(
    f"{BASE_URL}/validate-invoice-duplicate",
    headers=headers,
    json={"invoiceIds": [1, 2, 3]}
)
print(f"Submit response: {response.status_code}")
task_data = response.json()
print(f"Task data: {json.dumps(task_data, indent=2)}")

if "task_id" not in task_data:
    print("ERROR: Failed to submit task")
    exit(1)

task_id = task_data["task_id"]
print(f"\nTask ID: {task_id}")

# Step 3: Poll task status
print("\nStep 3: Polling task status...")
for i in range(10):
    print(f"\nPoll attempt {i+1}:")
    response = requests.get(f"{BASE_URL}/task-status/{task_id}")
    status_data = response.json()
    print(f"Status: {json.dumps(status_data, indent=2)}")
    
    if status_data.get("status") in ["SUCCESS", "FAILURE"]:
        print(f"\nTask completed with status: {status_data.get('status')}")
        if status_data.get("result"):
            print(f"Result: {json.dumps(status_data['result'], indent=2)}")
        if status_data.get("error"):
            print(f"Error: {status_data['error']}")
        break
    
    time.sleep(3)

print("\nTest completed!")
