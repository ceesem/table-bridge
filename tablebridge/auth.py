import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def get_credentials(token_file, client_secrets=None):
    creds = None
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)
    if not creds or not creds.valid:
        creds = refresh_credentials(creds, token_file, client_secrets)
    return creds


def refresh_credentials(creds, token_file, client_secrets=None):
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        if client_secrets is not None:
            flow = InstalledAppFlow.from_client_secrets_file(client_secrets, SCOPES)
            creds = flow.run_local_server(port=0)
        else:
            raise Exception("Could not get token")
    with open(token_file, "w") as token:
        token.write(creds.to_json())
    return creds
