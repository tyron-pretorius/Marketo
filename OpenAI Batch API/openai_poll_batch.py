from openai import OpenAI

client = OpenAI(api_key="xxx")

batch = client.batches.retrieve("batch_681b8165b2988190a18115853ffa5358")
print(batch)
