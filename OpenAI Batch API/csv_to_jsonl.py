import csv
import json
import os
import tiktoken

INPUT_CSV = '/Users/tyronpretorius/Downloads/blog_post.csv'
OUTPUT_DIR = '/Users/tyronpretorius/Downloads/'

MODEL = "gpt-4o"  # Token estimator
MAX_TOKENS_PER_BATCH = 90000

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Initialize tiktoken encoder
enc = tiktoken.encoding_for_model(MODEL)

def estimate_tokens(text):
    return len(enc.encode(text))

def create_json_entry(row):
    messages = [
        {
            "role": "system",
            "content": (
                "You are a helpful assistant that formats phone numbers into E164 format. "
                "Your approach will always be to: "
                "1. See if you can convert the phone number to E164 using the 'Phone' field on its own "
                "2. If you cannot determine which country the phone number is from then you can use the 'Country' "
                "field in order to get the extension needed for E164 format "
                "3. If for some reason you are not able to determine the E164 format for a number then return 'Unknown' "
                "4. If there is an extension present in the phone number then add the extension to the E164 number following "
                "this format exactly 'E164 ext xxx'. You will only return the E164 formatted phone number"
            )
        },
        {
            "role": "user",
            "content": f"Phone: {row['Phone Number']}|Country: {row['Country']}"
        }
    ]

    return {
        "custom_id": row["Id"]+"|"+row["Email Address"],
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": MODEL,
            "messages": messages,
            "max_tokens": 12
        }
    }

def process_csv(input_csv):
    with open(input_csv, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)

        batch = []
        token_count = 0
        batch_index = 0

        for row in reader:
            json_entry = create_json_entry(row)

            # Estimate token count
            msg_tokens = sum(estimate_tokens(msg["content"]) + 4 for msg in json_entry["body"]["messages"])
            total_tokens = msg_tokens + 2  # Add a bit of buffer

            # Start a new batch if token limit exceeded
            if token_count + total_tokens > MAX_TOKENS_PER_BATCH:
                output_path = os.path.join(OUTPUT_DIR, f"batch_{batch_index}.jsonl")
                with open(output_path, 'w', encoding='utf-8') as outfile:
                    for item in batch:
                        outfile.write(json.dumps(item) + '\n')
                batch_index += 1
                batch = []
                token_count = 0

            batch.append(json_entry)
            token_count += total_tokens

        # Write final batch
        if batch:
            output_path = os.path.join(OUTPUT_DIR, f"batch_{batch_index}.jsonl")
            with open(output_path, 'w', encoding='utf-8') as outfile:
                for item in batch:
                    outfile.write(json.dumps(item) + '\n')

if __name__ == "__main__":
    process_csv(INPUT_CSV)
