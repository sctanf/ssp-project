import csv
from datasets import load_dataset

OUTPUT_FILE = 'task_4.csv'
HEADERS = [
    'PRID', 'PRSHA', 'PRCOMMITMESSAGE', 'PRFILE', 'PRSTATUS',
    'PRADDS', 'PRDELSS', 'PRCHANGECOUNT', 'PRDIFF'
]

def clean_string(text):
    """Removes special characters to avoid encoding errors."""
    if text is None:
        return ''
    # Encode to ASCII, ignoring errors, then decode back to UTF-8
    return text.encode('ascii', 'ignore').decode('utf-8')

try:
    dataset = load_dataset("hao-li/AIDev", 'pr_commit_details', streaming=True)
    stream = dataset['train']
except Exception as e:
    print(f"Error loading dataset: {e}")
    exit()

print(f"Starting to stream 'pr_commit_details' data to {OUTPUT_FILE}...")

with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(HEADERS)
    
    for i, row in enumerate(stream):
        try:
            writer.writerow([
                row.get('pr_id'),
                row.get('sha'),
                row.get('message'),
                row.get('filename'),
                row.get('status'),
                row.get('additions'),
                row.get('deletions'),
                row.get('changes'),
                clean_string(row.get('patch')) # Clean the diff/patch field
            ])
        except Exception as e:
            print(f"Error writing row {i}: {e}")
            
        if (i + 1) % 100 == 0:
            print(f"Processed {i+1} rows...           ", end='\r', flush=True)

print(f"Processed {i+1} rows...")
print(f"Task 4 complete. Data saved to {OUTPUT_FILE}")