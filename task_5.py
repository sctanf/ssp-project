import pandas as pd

# Keywords from the README
SECURITY_KEYWORDS = [
    'race', 'racy', 'buffer', 'overflow', 'stack', 'integer', 'signedness',
    'underflow', 'improper', 'unauthenticated', 'gain access', 'permission',
    'cross site', 'css', 'xss', 'denial service', 'dos', 'crash', 'deadlock',
    'injection', 'request forgery', 'csrf', 'xsrf', 'forged', 'security',
    'vulnerability', 'vulnerable', 'exploit', 'attack', 'bypass', 'backdoor',
    'threat', 'expose', 'breach', 'violate', 'fatal', 'blacklist', 'overrun',
    'insecure'
]

# Input files (from Task 1 and Task 3)
TASK_1_CSV = 'task_1.csv'
TASK_3_CSV = 'task_3.csv'
OUTPUT_FILE = 'task_5.csv'

def check_security(row):
    """Checks if title or body contain any security keywords."""
    # Combine title and body, convert to string (to handle None/NaN) and lower
    text_to_check = str(row['TITLE']).lower() + ' ' + str(row['BODYSTRING']).lower()
    
    for keyword in SECURITY_KEYWORDS:
        if keyword in text_to_check:
            return 1
    return 0

print(f"Loading {TASK_1_CSV} and {TASK_3_CSV}...")

try:
    # Load the CSVs
    df_task1 = pd.read_csv(TASK_1_CSV, dtype={'ID': str})
    df_task3 = pd.read_csv(TASK_3_CSV, dtype={'PRID': str})
except FileNotFoundError as e:
    print(f"Error: {e}. Please run task_1.py and task_3.py first.")
    exit()

print("Files loaded. Merging data...")

# Merge the dataframes on the pull request ID
# Task 1 uses 'ID', Task 3 uses 'PRID'
merged_df = pd.merge(
    df_task1,
    df_task3,
    left_on='ID',
    right_on='PRID',
    how='inner' # We only care about PRs present in both files
)

print("Data merged. Calculating SECURITY flag...")

# Calculate the SECURITY flag
merged_df['SECURITY'] = merged_df.apply(check_security, axis=1)

# Select and rename columns to match the Task-5 spec
final_df = merged_df[[
    'ID', 'AGENTNAME', 'PRTYPE', 'CONFIDENCE', 'SECURITY'
]]

final_df = final_df.rename(columns={
    'AGENTNAME': 'AGENT',
    'PRTYPE': 'TYPE'
})

# Save the final CSV
final_df.to_csv(OUTPUT_FILE, index=False)

print(f"Task 5 complete. Data saved to {OUTPUT_FILE}")