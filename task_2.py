import csv
from datasets import load_dataset

OUTPUT_FILE = 'task_2.csv'
HEADERS = ['REPOID', 'LANG', 'STARS', 'REPOURL']

try:
    dataset = load_dataset("hao-li/AIDev", 'all_repository', streaming=True)
    stream = dataset['train']
except Exception as e:
    print(f"Error loading dataset: {e}")
    exit()

print(f"Starting to stream 'all_repository' data to {OUTPUT_FILE}...")

with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(HEADERS)
    
    for i, row in enumerate(stream):
        try:
            writer.writerow([
                row.get('id'),
                row.get('language'),
                row.get('stars'),
                row.get('url')
            ])
        except Exception as e:
            print(f"Error writing row {i}: {e}")

        if (i + 1) % 100 == 0:
            print(f"Processed {i+1} rows...           ", end='\r', flush=True)

print(f"Processed {i+1} rows...")
print(f"Task 2 complete. Data saved to {OUTPUT_FILE}")