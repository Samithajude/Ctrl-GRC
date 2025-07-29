CTRL+GRC – Cybersecurity GRC Scoring and Reporting Tool

CTRL+GRC is a real-time Cybersecurity Governance, Risk, and Compliance (GRC) assessment system powered by Google Forms + Sheets, Python scoring logic, automated PDF report generation, and email delivery. Built to help startup and msmse organizations quickly evaluate their security posture.

Features:

⏱️ Real-time monitoring of Google Form responses

📊 Domain-wise security scoring (Policies, Risk Management, Compliance, etc.)

📄 PDF report generation using Jinja2 + HTML + wkhtmltopdf

📧 Auto-emailing personalized reports to form submitters

🌐 Flask-based web app hosted on Render

🔐 Secure config via environment variables and service account management

Project Structure:

grc_scoring.py (scoring, PDF generation, email generation)

scoring_config.json (Scoring rules per question/domain)

report_template.html (HTML report template for Jinja2)

requirements.txt (Python dependencies)

service_account.json ( see the steps below to obtain this)

README.md

Prerequisites:

Google Form connected to a Google Sheet

Service account with Sheets API access

Google Sheet share access to the service account email

Gmail App Password (for sending email)

scoring_config.json aligned with your Form questions

Render.com account for hosting (or local setup)

Local Installation:

1.Clone the repo:

git clone https://github.com/rohitharavinth/CTRL-GRC.git cd CTRL-GRC

2.Install dependencies:

pip install -r requirements.txt

3.Install wkhtmltopdf:

Ubuntu/Debian: sudo apt install wkhtmltopdf Windows: Download from https://wkhtmltopdf.org/downloads.html and Add to your system PATH.

4.service_account.json creation

Step 1: Go to Google Cloud Console > Sign in with your Gmail account. > Create a new Google Cloud Project (or use an existing one).

Step 2: Enable Google Sheets API > In the left sidebar, go to APIs & Services → Library. > Search for Google Sheets API. > Click Enable.

Step 3: Create a Service Account > Go to IAM & Admin → Service Accounts. > Click “Create Service Account”.

Name: ctrl-grc-sheets-access (or anything) > Description: For accessing Google Sheets from Python

Click Create and Continue.

Step 4: Grant Permissions > In the "Grant this service account access to project" screen:

Role: Choose Editor (you can later restrict this further to just "Viewer").

Click Done.

Step 5: Generate Service Account JSON Key > Go back to IAM & Admin → Service Accounts.

Click on the service account you just created. > Go to the "Keys" tab. > Click "Add Key" → "Create new key". > Select JSON format. > Click Create.

A .json file will download automatically — this is your service_account.json file.

Step 6: Share Your Google Sheet with the Service Account

Open your linked Google Sheet and Click “Share”. > Add the service account email (it looks like: your-service-name@your-project-id.iam.gserviceaccount.com).

Give it Viewer access (or Editor if needed).

Save.

Step 7 Final Tips:

Rename the downloaded file to service_account.json.

In your Python project, place it in a safe path or secure folder.

For Render.com:(optional)

Go to Secrets → upload it.

Mount it to /etc/secrets/service_account.json.

Then in your Python file change:

SERVICE_ACCOUNT_FILE = "/etc/secrets/service_account.json"

Email id and password integration in your python code
Set environment variables (for Gmail sender): change the email and password

export EMAIL_USER="your_email@gmail.com" export EMAIL_PASS="your_app_password"

password should be app based password which can be generated in google accounts.

6.Run python grc_scoring.py

Optional: Google Apps Script Trigger To trigger your /generate endpoint from Google Form:
function onFormSubmit(e) { var options = { "muteHttpExceptions": true }; UrlFetchApp.fetch("https://ctrl-grc.onrender.com/generate", options); }

Attach this to your Form's "On form submit" trigger.

Sample Report
The generated PDF includes:

Overall security score and percentage Domain-wise score breakdown Executive summary Recommendations with priority (Critical, High, Medium, Low)

Troubleshooting
❌ 404 on /generate: Check app.py for correct route method and deployment.

❌ Invalid JWT: Make sure the service account JSON is valid and has Sheet access.

❌ Email not sending: Ensure app password is valid, and sender email is not blocked.

Built with ❤️ and passion for cybersecurity by @rohitharavinth and @Samithajude
