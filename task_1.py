import csv
from datasets import load_dataset

# Define the output CSV file and headers
OUTPUT_FILE = 'task_1.csv'
HEADERS = ['TITLE', 'ID', 'AGENTNAME', 'BODYSTRING', 'REPOID', 'REPOURL']

# Load the dataset in streaming mode
try:
    dataset = load_dataset("hao-li/AIDev", 'all_pull_request', streaming=True)
    stream = dataset['train'] # Access the default data split
except Exception as e:
    print(f"Error loading dataset: {e}")
    exit()

print(f"Starting to stream 'all_pull_request' data to {OUTPUT_FILE}...")

# Open the CSV file for writing
with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(HEADERS)
    
    # Iterate over the streaming dataset
    for i, row in enumerate(stream):
        try:
            writer.writerow([
                row.get('title'),
                row.get('id'),
                row.get('agent'),
                row.get('body'),
                row.get('repo_id'),
                row.get('repo_url')
            ])
        except Exception as e:
            print(f"Error writing row {i}: {e}")
            # Continue to the next row
            
        if (i + 1) % 100 == 0:
            print(f"Processed {i+1} rows...           ", end='\r', flush=True)

print(f"Processed {i+1} rows...")
print(f"Task 1 complete. Data saved to {OUTPUT_FILE}")