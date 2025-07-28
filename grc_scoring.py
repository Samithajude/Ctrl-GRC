import json
import gspread
import pdfkit
import ast
import os
import re
import smtplib
import pandas as pd
from jinja2 import Environment, FileSystemLoader
from email.message import EmailMessage
from google.oauth2.service_account import Credentials

# ---------------------------
# Configuration
# ---------------------------
SHEET_URL = 'https://docs.google.com/spreadsheets/d/1lMN0t_y0YRRXq7TurJZI3iqQmrsejfYgSYW8FtfkCTU/edit?resourcekey=&gid=2058047489#gid=2058047489' # Update this line
SERVICE_ACCOUNT_FILE = 'service_account.json'
SCORING_CONFIG_FILE = 'scoring_config.json'
REPORT_TEMPLATE = 'report_template.html'
OUTPUT_FILE = 'report.html'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
WKHTMLTOPDF_PATH = r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe"

# Email Config
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587
EMAIL_SENDER = 'ctrl.grc714@gmail.com'
EMAIL_PASSWORD = 'usme dlff patd aumk'  # App password

# ---------------------------
# Load scoring config
# ---------------------------
def load_scoring_config(config_file):
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Scoring config file not found: {config_file}")
    with open(config_file, 'r', encoding='utf-8') as f:
        return json.load(f)

# ---------------------------
# Fetch data from Google Sheet
# ---------------------------
def fetch_sheet_data(sheet_url, service_account_file, scopes):
    creds = Credentials.from_service_account_file(service_account_file, scopes=scopes)
    client = gspread.authorize(creds)
    worksheet = client.open_by_url(sheet_url).worksheet('Form Responses 1')
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)
    if df.empty:
        raise ValueError("No data found in the Google Sheet.")
    print(f"✅ Loaded {len(df)} responses from Google Sheet.")
    return df

# ---------------------------
# Process a single response
# ---------------------------
def process_response(row, scoring_config):
    domain_scores, domain_max_scores, domain_recommendations = {}, {}, {}
    total_score, max_total = 0, 0

    multi_select_questions = [
        "Which do you see as top threats to your business?",
        "What technical controls are currently in place?"
    ]

    for q in scoring_config:
        q_text = q["question"]
        raw_answer = str(row.get(q_text, "")).strip()
        domain, max_score, weight = q["domain"], q["max_score"], q.get("weight", 1)
        question_score = 0

        # Handle multiselect
        if q_text in multi_select_questions:
            selected_options = []
            if raw_answer.startswith("[") and raw_answer.endswith("]"):
                try:
                    parsed = ast.literal_eval(raw_answer)
                    if isinstance(parsed, list):
                        selected_options = [str(opt).strip() for opt in parsed]
                except Exception:
                    selected_options = [raw_answer]
            else:
                selected_options = [opt.strip() for opt in raw_answer.split(",") if opt.strip()]
            question_score = sum(q["rules"].get(opt, 0) for opt in selected_options)
            answer_display = ", ".join(selected_options) if selected_options else "No answer provided"
        else:
            question_score = q["rules"].get(raw_answer, 0)
            answer_display = raw_answer if raw_answer else "No answer provided"

        weighted_score = question_score * weight
        weighted_max = max_score * weight
        total_score += weighted_score
        max_total += weighted_max
        domain_scores[domain] = domain_scores.get(domain, 0) + weighted_score
        domain_max_scores[domain] = domain_max_scores.get(domain, 0) + weighted_max

        if weighted_score < weighted_max:
            rec_text = q.get("best_practices", {}).get(raw_answer, "Review and improve controls in this area.")
            priority = get_recommendation_priority(weighted_score, weighted_max, weight)
            domain_recommendations.setdefault(domain, []).append({
                "question": q_text,
                "answer": answer_display,
                "how_to": rec_text,
                "priority": priority
            })

    percentage = round((total_score / max_total) * 100, 1) if max_total else 0
    domain_percentages = {
        d: round((domain_scores[d] / domain_max_scores[d]) * 100, 1) if domain_max_scores[d] else 0
        for d in domain_scores
    }
    executive_summary = generate_executive_summary(percentage)

    return total_score, max_total, percentage, domain_scores, domain_max_scores, domain_percentages, domain_recommendations, executive_summary

def get_recommendation_priority(score, max_score, weight):
    if score == 0: return "Critical"
    elif score < max_score / 2: return "High"
    elif score < max_score: return "Medium"
    else: return "Low"

def generate_executive_summary(percentage):
    if percentage >= 80:
        return "Your organization shows a strong security posture. Continue regular reviews and improvements."
    elif percentage >= 50:
        return "Your organization has a moderate security posture. Some good practices are in place, but key areas need attention."
    else:
        return "Your organization has a weak security posture. Critical improvements are urgently needed to reduce risk."

def render_pdf_report(template_file, pdf_output_file, context):
    env = Environment(loader=FileSystemLoader('.'))
    template = env.get_template(template_file)
    html = template.render(context)
    config = pdfkit.configuration(wkhtmltopdf=WKHTMLTOPDF_PATH)
    pdfkit.from_string(html, pdf_output_file, configuration=config)
    print(f"✅ PDF report generated: {pdf_output_file}")

def send_email_with_pdf(receiver_email, subject, body, pdf_path):
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = EMAIL_SENDER
    msg['To'] = receiver_email
    msg.set_content(body)

    with open(pdf_path, 'rb') as f:
        msg.add_attachment(f.read(), maintype='application', subtype='pdf', filename=os.path.basename(pdf_path))

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as smtp:
        smtp.starttls()
        smtp.login(EMAIL_SENDER, EMAIL_PASSWORD)
        smtp.send_message(msg)
        print(f"📧 Report sent to {receiver_email}")

# ---------------------------
# Main
# ---------------------------
if __name__ == "__main__":
    try:
        scoring = load_scoring_config(SCORING_CONFIG_FILE)
        df = fetch_sheet_data(SHEET_URL, SERVICE_ACCOUNT_FILE, SCOPES)

        # Show all responses
        print("\nSelect the response to generate report:")
        for idx, email in enumerate(df["Email Address"]):
            print(f"{idx}: {email}")
        selected_index = int(input("\nEnter the response index number: "))
        selected_row = df.loc[selected_index]

        print(f"\nGenerating report for: {selected_row['Email Address']}")

        total, max_total, pct, dom_scores, dom_max, dom_pct, dom_recs, summary = process_response(selected_row, scoring)
        timestamp = pd.Timestamp.now().strftime("%Y-%m-%d_%H-%M-%S")
        pdf_output_file = f"Cybersecurity_GRC_Report_{timestamp}.pdf"

        context = {
            "total_score": total,
            "max_total": max_total,
            "percentage": pct,
            "domain_scores": dom_scores,
            "domain_max_scores": dom_max,
            "domain_percentages": dom_pct,
            "domain_recommendations": dom_recs,
            "executive_summary": summary,
            "current_date": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        render_pdf_report(REPORT_TEMPLATE, pdf_output_file, context)

        send_email_with_pdf(
            receiver_email=selected_row["Email Address"],
            subject="Your Cybersecurity GRC Report",
            body="Dear user,\n\nPlease find attached your personalized cybersecurity assessment report.\n\nBest regards,\nCTRL+GRC",
            pdf_path=pdf_output_file
        )

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
