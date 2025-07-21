import requests

access_token = '3o4rODDGxl9yj3WOqIxEfF9HMPryX2V0AAAAAQoXIiAAAAGXWK8S8c2yTeNnt1bO'
headers = {
    "Authorization": f"Bearer {access_token}"
}

response = requests.get("https://www.example.com/oauth/v1/api/talk/profile", headers=headers)
print(response.status_code)
print(response.json())