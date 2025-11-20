import subprocess
import sys
import os

# Define the list of scripts to run in the correct order
scripts_to_run = [
    "task_1.py",
    "task_2.py",
    "task_3.py",
    "task_4.py",
    "task_5.py"
]

# Get the path to the current Python interpreter
python_executable = sys.executable

print(f"Starting data processing pipeline using: {python_executable}\n")

# Loop through each script and execute it
for script_name in scripts_to_run:
    print("-" * 50)
    print(f"[+] Starting: {script_name}")
    print("-" * 50)
    
    # Check if the file exists before trying to run it
    if not os.path.exists(script_name):
        print(f"\n[ERROR] File not found: {script_name}")
        print("Pipeline aborted.")
        break # Stop the loop

    try:
        result = subprocess.run(
            [python_executable, script_name],
            check=True # Still check for errors
        )
        
        # We no longer print result.stdout, as it's already been printed.
        print(f"\n[SUCCESS] Finished: {script_name}")

    except subprocess.CalledProcessError as e:
        # The script's specific error message was already printed
        # in real-time because we didn't capture output.
        print(f"\n[ERROR] {script_name} failed with exit code {e.returncode}.")
        print("Pipeline aborted.")
        break # Stop the loop
        
    except FileNotFoundError:
        print(f"\n[ERROR] Could not find interpreter: {python_executable}")
        print("Pipeline aborted.")
        break
        
    except Exception as e:
        print(f"\n[ERROR] An unexpected error occurred while running {script_name}: {e}")
        print("Pipeline aborted.")
        break

else:
    # This 'else' block runs only if the loop completed without 'break'
    print("-" * 50)
    print("✅ All tasks completed successfully.")