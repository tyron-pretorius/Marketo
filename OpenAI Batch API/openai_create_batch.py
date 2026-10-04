from openai import OpenAI


client = OpenAI(api_key="xxx")

batch_input_file = client.files.create(
    file=open("/Users/tyronpretorius/Downloads/hear_about_batch_api_0.jsonl", "rb"),
    purpose="batch"
)

print(batch_input_file)

batch_input_file_id = batch_input_file.id
response = client.batches.create(
    input_file_id=batch_input_file_id,
    endpoint="/v1/chat/completions",
    completion_window="24h",
    metadata={
        "description": "xxxx"
    }
)

print(response)
