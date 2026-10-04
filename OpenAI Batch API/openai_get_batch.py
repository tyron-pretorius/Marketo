import pandas as pd
import json
from openai import OpenAI

# Initialize OpenAI client
client = OpenAI(api_key="xxx")

# Load output file from OpenAI
output_file_id = "file-Vwiv9sUhPUDBD9KTNEmKT6"
output_file = client.files.content(output_file_id)
with open("/Users/tyronpretorius/Downloads/blog_post_output.jsonl", "w") as f:
    f.write(output_file.text)

# Parse the response data
lines = output_file.text.strip().split("\n")
data = [json.loads(line) for line in lines]

# Process each record
records = []
for item in data:
    try:
        # Split custom_id into id and email
        custom_id = item.get("custom_id", "")
        id_part, email = custom_id.split("|") if "|" in custom_id else (custom_id, "Unknown")

        content = item["response"]["body"]["choices"][0]["message"]["content"]
        prompt_tokens = item["response"]["body"]["usage"]["prompt_tokens"]
        completion_tokens = item["response"]["body"]["usage"]["completion_tokens"]
        total_tokens = item["response"]["body"]["usage"]["total_tokens"]

        # Cost calculations
        cost_prompt = (prompt_tokens / 1_000_000) * 1.25
        cost_completion = (completion_tokens / 1_000_000) * 5
        total_cost = cost_prompt + cost_completion

        records.append({
            "Id": id_part,
            "Email Address": email,
            "Phone Number": content,
            "Input Tokens": prompt_tokens,
            "Output Tokens": completion_tokens,
            "Total Cost": round(total_cost, 6)
        })
    except Exception as e:
        print(f"Error processing item with custom_id {item.get('custom_id')}: {e}")

# Create DataFrame and save to CSV
df = pd.DataFrame(records)
df.to_csv("/Users/tyronpretorius/Downloads/batch_results_gpt4o.csv", index=False)

print("Results saved to batch_results_gpt4o.csv")
