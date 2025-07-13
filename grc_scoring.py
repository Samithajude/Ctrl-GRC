import json
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from jinja2 import Environment, FileSystemLoader
import os

# ---------------------------
# Configuration
# ---------------------------
# ---------------------------
# Configuration
# ---------------------------
SHEET_URL = 'https://docs.google.com/spreadsheets/d/1lMN0t_y0YRRXq7TurJZI3iqQmrsejfYgSYW8FtfkCTU/edit?resourcekey=&gid=2058047489#gid=2058047489' # Update this line
SERVICE_ACCOUNT_FILE = 'service_account.json'
SCORING_CONFIG_FILE = 'scoring_config.json'
REPORT_TEMPLATE = 'report_template.html'
OUTPUT_FILE = 'report.html'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# ---------------------------
# Functions for Modularity
# ---------------------------

def load_scoring_config(config_file):
    """Loads the scoring configuration from a JSON file."""
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Scoring configuration file not found: {config_file}")
    with open(config_file, 'r', encoding='utf-8') as f:
        return json.load(f)

def fetch_sheet_data(sheet_url, service_account_file, scopes):
    """Authenticates with Google Sheets and fetches data."""
    if not os.path.exists(service_account_file):
        raise FileNotFoundError(f"Service account file not found: {service_account_file}")
    try:
        creds = Credentials.from_service_account_file(service_account_file, scopes=scopes)
        client = gspread.authorize(creds)
        # Assuming the first worksheet is 'Form Responses 1'
        worksheet = client.open_by_url(sheet_url).worksheet('Form Responses 1')
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)
        if df.empty:
            raise ValueError("No data found in the Google Sheet.")
        print(f"✅ Loaded {len(df)} responses from Google Sheet.")
        return df
    except gspread.exceptions.SpreadsheetNotFound:
        raise ValueError(f"Google Sheet not found at URL: {sheet_url}. Check URL and permissions.")
    except gspread.exceptions.NoValidUrlKeyFound:
        raise ValueError(f"Invalid Google Sheet URL: {sheet_url}.")
    except Exception as e:
        raise Exception(f"Failed to fetch data from Google Sheet: {e}")

def get_recommendation_priority(current_score, max_score, weight):
    """Determines the priority of a recommendation based on score deficit."""
    if current_score == 0:
        return "Critical"
    elif current_score < max_score / 2: # Scored less than half
        return "High"
    elif current_score < max_score: # Scored something but not max
        return "Medium"
    else:
        return "Low" # Should not typically trigger a recommendation, but for completeness

def process_latest_response(df, scoring_config):
    """Processes the latest response to calculate scores and recommendations."""
    latest_row = df.iloc[-1]

    domain_scores = {}
    domain_max_scores = {}
    domain_recommendations = {}
    total_score = 0
    max_total = 0

    for q in scoring_config:
        q_text = q["question"]
        answer = str(latest_row.get(q_text, "")).strip()
        domain = q["domain"]
        max_score = q["max_score"]
        weight = q.get("weight", 1) # Get weight, default to 1 if not specified

        question_score = 0
        
        # Handle multi-select answers (comma-separated string from Google Forms)
        if "," in answer and q_text in ["Which do you see as top threats to your business?", "What technical controls are currently in place?"]:
            selected_options = [opt.strip() for opt in answer.split(',')]
            for opt in selected_options:
                question_score += q["rules"].get(opt, 0)
        else: # Single select answer
            question_score = q["rules"].get(answer, 0)
        
        # Apply weight to the score and max_score
        weighted_question_score = question_score * weight
        weighted_max_score = max_score * weight

        total_score += weighted_question_score
        max_total += weighted_max_score

        domain_scores[domain] = domain_scores.get(domain, 0) + weighted_question_score
        domain_max_scores[domain] = domain_max_scores.get(domain, 0) + weighted_max_score

        # Add recommendation if not full score, considering weighted score for priority
        if weighted_question_score < weighted_max_score:
            best_practices = q.get("best_practices", {})
            current_recommendation_text = None

            # Logic for multi-select questions with specific best practices
            if "," in answer and q_text in ["Which do you see as top threats to your business?", "What technical controls are currently in place?"]:
                selected_options = [opt.strip() for opt in answer.split(',')]
                
                recommended_for_missing = []
                for rule_key, rule_score in q["rules"].items():
                    if rule_score > 0 and rule_key not in selected_options and rule_key in best_practices:
                        recommended_for_missing.append(best_practices[rule_key])

                if not recommended_for_missing and "None / Not sure" in best_practices and not answer:
                    current_recommendation_text = best_practices["None / Not sure"]
                elif recommended_for_missing:
                    current_recommendation_text = "; ".join(recommended_for_missing)

            # Logic for single-select questions or fallback for multi-select
            if not current_recommendation_text:
                if answer in best_practices: # Exact match for answer provided
                    current_recommendation_text = best_practices[answer]
                else: # Fallback to general if no specific match for the given answer
                    current_recommendation_text = "Review and improve controls in this area."
            
            priority = get_recommendation_priority(weighted_question_score, weighted_max_score, weight)

            domain_recommendations.setdefault(domain, []).append({
                "question": q_text,
                "answer": answer or "No answer provided",
                "how_to": current_recommendation_text,
                "priority": priority
            })

    # Calculate final percentages
    percentage = round((total_score / max_total) * 100, 1) if max_total else 0
    domain_percentages = {
        domain: round((domain_scores[domain] / domain_max_scores[domain]) * 100, 1)
        if domain_max_scores[domain] else 0
        for domain in domain_scores
    }

    # Generate Executive Summary based on overall score
    executive_summary = generate_executive_summary(percentage, domain_recommendations)

    return total_score, max_total, percentage, domain_scores, domain_max_scores, domain_percentages, domain_recommendations, executive_summary

def generate_executive_summary(overall_percentage, recommendations):
    """Generates a brief executive summary."""
    summary = ""
    if overall_percentage >= 80:
        summary = "Your MSME demonstrates a **strong security posture** with robust controls in place. Continue to monitor and adapt to emerging threats, focusing on minor areas for optimization."
    elif overall_percentage >= 50:
        summary = "Your MSME has a **moderate security posture**. While many foundational controls are present, there are significant gaps, particularly in the areas highlighted in the recommendations, that require immediate attention to mitigate risks."
    else:
        summary = "Your MSME's security posture requires **urgent and significant improvement**. Critical vulnerabilities are likely present, and immediate action on the high-priority recommendations is essential to prevent potential security incidents and data breaches."
    
    # Add top 3 critical recommendations to summary
    critical_recs = []
    for domain, recs in recommendations.items():
        for rec in recs:
            if rec['priority'] == 'Critical':
                critical_recs.append(f"- {rec['question']}: {rec['how_to']}")
            if len(critical_recs) >= 3:
                break
        if len(critical_recs) >= 3:
            break
    
    if critical_recs:
        summary += "\n\n**Key Areas for Immediate Focus:**\n" + "\n".join(critical_recs)
    
    return summary


def render_html_report(template_file, output_file, context_data):
    """Renders the HTML report using Jinja2."""
    if not os.path.exists(template_file):
        raise FileNotFoundError(f"Report template file not found: {template_file}")
    env = Environment(loader=FileSystemLoader('.'))
    template = env.get_template(template_file)

    html_content = template.render(context_data)

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"\n✅ HTML report generated: {output_file}")
    print(f"📊 Overall score: {context_data['total_score']}/{context_data['max_total']} ({context_data['percentage']}%)")


# ---------------------------
# Main Execution
# ---------------------------
if __name__ == "__main__":
    try:
        scoring_config = load_scoring_config(SCORING_CONFIG_FILE)
        df = fetch_sheet_data(SHEET_URL, SERVICE_ACCOUNT_FILE, SCOPES)

        total_score, max_total, percentage, domain_scores, domain_max_scores, domain_percentages, domain_recommendations, executive_summary = \
            process_latest_response(df, scoring_config)
        
        context = {
            "total_score": total_score,
            "max_total": max_total,
            "percentage": percentage,
            "domain_scores": domain_scores,
            "domain_max_scores": domain_max_scores,
            "domain_percentages": domain_percentages,
            "domain_recommendations": domain_recommendations,
            "executive_summary": executive_summary,
            "current_date": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S") # For footer
        }

        render_html_report(REPORT_TEMPLATE, OUTPUT_FILE, context)

    except FileNotFoundError as e:
        print(f"ERROR: {e}")
    except ValueError as e:
        print(f"ERROR: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")