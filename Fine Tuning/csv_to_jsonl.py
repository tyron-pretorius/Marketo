import pandas as pd
import json
import re
import random

############ Updates Needed Here #############

SYSTEM_PROMPT = """You’re a digital marketing expert whose task is to take the free-text answer to “How did you hear about Telnyx?” 
and categorize it into one of these 8 options: Digital Advertising, Inbound, Organic, Paid Search, Referral, Sales, Tradeshow, Unknown"""

INPUT_CSV_FILE = "/Users/tyronpretorius/Downloads/ha_input_2025050701.csv"
TRAIN_OUTPUT_FILE = "/Users/tyronpretorius/Downloads/ha_train4.jsonl"
VALIDATE_OUTPUT_FILE = "/Users/tyronpretorius/Downloads/ha_validate4.jsonl"
SKIPPED_LOG_FILE = "/Users/tyronpretorius/Downloads/skipped_lines4.txt"

#############################################

def sanitize_text(text):
    if pd.isna(text):
        return ""
    if not isinstance(text, str):
        text = str(text)

    # Remove control characters except \n and \t
    text = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f]', '', text)

    # Normalize whitespace
    text = text.strip().replace('\r\n', '\n').replace('\r', '\n')

    # Escape unescaped double quotes (basic fix, better than nothing)
    text = text.replace('\\"', '"')  # Undo existing escapes to avoid doubling
    text = text.replace('"', '\\"')  # Re-escape
    return text

def validate_json_line(json_line):
    try:
        json.loads(json_line)
        return True
    except json.JSONDecodeError:
        return False

def csv_to_jsonl_split(input_file, train_file, validate_file, skipped_log_file, system_prompt):
    df = pd.read_csv(input_file)
    skipped_lines = []

    with open(train_file, 'w', encoding='utf-8') as train_out, \
         open(validate_file, 'w', encoding='utf-8') as validate_out:

        for i, row in df.iterrows():
            user_text = sanitize_text(row.get("user", ""))
            assistant_text = sanitize_text(row.get("assistant", ""))

            json_obj = {
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_text},
                    {"role": "assistant", "content": assistant_text}
                ]
            }

            json_line = json.dumps(json_obj, ensure_ascii=False)

            if validate_json_line(json_line):
                # 80% to training, 20% to validation
                if random.random() < 0.8:
                    train_out.write(json_line + "\n")
                else:
                    validate_out.write(json_line + "\n")
            else:
                skipped_lines.append((i + 1, user_text, assistant_text))

    # Log skipped lines
    if skipped_lines:
        with open(skipped_log_file, 'w', encoding='utf-8') as log:
            for line_num, user, assistant in skipped_lines:
                log.write(f"Line {line_num} skipped:\nUser: {user}\nAssistant: {assistant}\n\n")

if __name__ == "__main__":
    csv_to_jsonl_split(INPUT_CSV_FILE, TRAIN_OUTPUT_FILE, VALIDATE_OUTPUT_FILE, SKIPPED_LOG_FILE, SYSTEM_PROMPT)
