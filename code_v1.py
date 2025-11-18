import os
import time
import threading
import requests
from flask import Flask, redirect, request
from requests_oauthlib import OAuth2Session
from bs4 import BeautifulSoup
from databricks.sdk import WorkspaceClient
import pandas as pd
import tabulate as tb

app = Flask(__name__) # Flask instance created with the directory of the current file
app.secret_key = "dev" #temporary secret key

# INPUTS
CLIENT_ID = "<Enter your Client ID>"
CLIENT_SECRET = "<Enter your Client Secret>"
REDIRECT_URI = "http://localhost:5000/callback" 
SCOPE = ["Chat.ReadWrite"]
TENANT_ID = "<Enter your Tenant ID>"
authorization_base_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/authorize"
token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
CHAT_ID = "<Enter your Chat ID>"
DATABRICKS_HOST = "<Enter your Databricks Host name>"
DATABRICKS_TOKEN = "<Enter your Databricks Token>"
OMIT_USER_ID = "<Enter your Microsoft Teams ID to be ommited>"

# Interaction with Databricks Genie
client = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)
genie = client.genie

# Setting environment variables
os.environ["DATABRICKS_HOST"] = DATABRICKS_HOST
os.environ["DATABRICKS_TOKEN"] = DATABRICKS_TOKEN

# Global variable to store OAuth token and headers
access_token = None
headers = None
last_message_id = None

# HTML to the text by removing html tags
def extract_text_from_html(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    return soup.get_text()

# Providing input to Genie & Getting the output from it
def ask_genie(message_text):
    spaces = genie.list_spaces().spaces # Look for the available Genie spaces
    if not spaces:
        return "No Genie spaces found."
    space = spaces[0] 
    message_text = f"'{message_text}'"
    response = genie.start_conversation_and_wait(space_id=space.space_id, content=message_text)
    cond = response.attachments[0].text
    if cond is not None:
        attachment_text = response.attachments[0].text.content.strip()
    else:
        response_1 = genie.get_message_query_result_by_attachment(space_id=space.space_id, conversation_id=response.conversation_id, message_id=response.message_id, attachment_id=response.attachments[0].attachment_id)
        col_nm = []
        resp = response_1.statement_response.result.data_array
        for col in response_1.statement_response.manifest.schema.columns:
            col_nm.append(col.name)
        attachment_text = pd.DataFrame(resp, columns=col_nm)
        attachment_text = attachment_text.to_dict(orient='records')
    return attachment_text

# Home Function
@app.route('/') # This tells the Flask to run the Home Function, when the user hits the root URL
def index():
    oauth = OAuth2Session(CLIENT_ID, redirect_uri=REDIRECT_URI, scope=SCOPE)
    authorization_url, state = oauth.authorization_url(authorization_base_url)
    return redirect(authorization_url)

# Callback Function
@app.route('/callback') # This tells the Flask to run the Callback Function, when the callback URL is called
def callback():
    global access_token, headers
    error = request.args.get('error')
    if error:
        return f"Auth failed: {error}", 400
    code = request.args.get('code')
    if not code:
        return "No auth code received.", 400
    oauth = OAuth2Session(CLIENT_ID, redirect_uri=REDIRECT_URI)
    token = oauth.fetch_token(token_url, client_secret=CLIENT_SECRET, code=code)
    access_token = token.get('access_token')
    headers = {"Authorization": f"Bearer {access_token}"}
    return "Authentication successful! You can now close this page."

def poll_teams_chat():
    global last_message_id
    while True:
        if headers is None:
            print("Access token not set. Waiting for OAuth login...")
            time.sleep(5)
            continue
        messages_url = f"https://graph.microsoft.com/v1.0/chats/{CHAT_ID}/messages?$top=1&$orderby=createdDateTime desc"
        resp = requests.get(messages_url, headers=headers)
        if resp.status_code != 200:
            print(f"Failed to fetch messages: {resp.text}")
            time.sleep(5)
            continue
        messages = resp.json().get("value", [])
        if not messages:
            print("No messages found.")
            time.sleep(5)
            continue
        latest_msg = messages[0]
        msg_id = latest_msg.get("id")
        sender_id = latest_msg.get("from", {}).get("user", {}).get("id") # Checking if the message sender is the user to omit
        if sender_id == OMIT_USER_ID:
            print(f"Skipping message from user {sender_id}")
            last_message_id = msg_id  # Update so we don't reprocess
            time.sleep(5)
            continue
        if msg_id == last_message_id:
            print("No new message.")
            time.sleep(5)
            continue
        last_message_id = msg_id
        latest_msg_text = extract_text_from_html(latest_msg.get("body", {}).get("content", "")).strip()
        print(f"New message from Teams: {latest_msg_text}")
        reply = ask_genie(latest_msg_text)
        if isinstance(reply, str):
            reply = reply.strip()
        else:
            reply = pd.DataFrame(reply)
            reply = tb.tabulate(reply, headers='keys', tablefmt='grid', showindex=False, floatfmt=".2f")
        print(f"Replying with: {reply}")
        post_url = f"https://graph.microsoft.com/v1.0/chats/{CHAT_ID}/messages"
        if isinstance(reply, str):
            reply_html = reply.replace("\n", "<br>")
        else:
            reply_html = str(reply).replace("\n", "<br>")
        post_data = {
            "body": {
                "contentType": "html",  # Use HTML if you want to include links or formatted replies
                "content": reply_html  # Convert line breaks to <br> for Teams rendering
    }
}
        post_resp = requests.post(post_url, headers={**headers, "Content-Type": "application/json"}, json=post_data)
        if post_resp.status_code >= 400:
            print(f"Failed to post reply: {post_resp.text}")
        else:
            print("Reply posted successfully.")
        time.sleep(5)


if __name__ == '__main__':
    threading.Thread(target=poll_teams_chat, daemon=True).start() 
    app.run(debug=True)