import pandas as pd
import json
from openai import OpenAI

# Initialize OpenAI client
client = OpenAI(api_key="xxx")

# Load output file from OpenAI
output_file_id = "file-8CzixDCGECAb4xbpds9k35"
output_file = client.files.content(output_file_id)
with open("/Users/tyronpretorius/Downloads/batch_output_1.csv", "w") as f:
    f.write(output_file.text)

# Read ID-Email mapping CSV
email_mapping_path = "/Users/tyronpretorius/Downloads/id_email_mapping.csv"
email_df = pd.read_csv(email_mapping_path)
email_dict = dict(zip(email_df['Id'].astype(str), email_df['Email Address']))

# Parse the response data
lines = output_file.text.strip().split("\n")
data = [json.loads(line) for line in lines]

# Process each record
records = []
for item in data:
    try:
        custom_id = item.get("custom_id")
        content = item["response"]["body"]["choices"][0]["message"]["content"]
        prompt_tokens = item["response"]["body"]["usage"]["prompt_tokens"]
        completion_tokens = item["response"]["body"]["usage"]["completion_tokens"]
        total_tokens = item["response"]["body"]["usage"]["total_tokens"]

        # Cost calculations
        cost_prompt = (prompt_tokens / 1_000_000) * 1.25
        cost_completion = (completion_tokens / 1_000_000) * 5
        total_cost = cost_prompt + cost_completion

        # Lookup email
        email = email_dict.get(custom_id, "Unknown")

        records.append({
            "id": custom_id,
            "email": email,
            "phone": content,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
            "cost_usd": round(total_cost, 6)
        })
    except Exception as e:
        print(f"Error processing item with id {item.get('custom_id')}: {e}")

# Create DataFrame and save to CSV
df = pd.DataFrame(records)
df.to_csv("/Users/tyronpretorius/Downloads/batch_results_gpt4o.csv", index=False)

print("Results saved to batch_results_gpt4o.csv")
