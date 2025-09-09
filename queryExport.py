import requests
from msal import ConfidentialClientApplication

# ---- Config ----
TENANT_ID     = "your-tenant-id"
CLIENT_ID     = "your-client-id"
CLIENT_SECRET = "your-client-secret"

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE     = ["https://api.security.microsoft.com/.default"]
ENDPOINT  = "https://api.security.microsoft.com/api/advancedhunting/run"

# ---- Auth ----
app = ConfidentialClientApplication(
    CLIENT_ID,
    authority=AUTHORITY,
    client_credential=CLIENT_SECRET
)
token_result = app.acquire_token_for_client(scopes=SCOPE)
if "access_token" not in token_result:
    raise SystemExit(f"Failed to get token: {token_result}")

access_token = token_result["access_token"]

# ---- Query ----
query = """
DeviceProcessEvents
| where InitiatingProcessFileName =~ "powershell.exe"
| project Timestamp, FileName, InitiatingProcessFileName
| order by Timestamp desc
| limit 2
"""

headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}
body = {"Query": query}

resp = requests.post(ENDPOINT, headers=headers, json=body)
resp.raise_for_status()
data = resp.json()

# ---- Print results ----
for row in data.get("Results", []):
    print(row)
