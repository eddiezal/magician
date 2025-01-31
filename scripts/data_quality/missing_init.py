import os

# Define directories where __init__.py is needed
dirs = [
    "scripts",
    "scripts/bigquery",
    "scripts/data_quality",
    "scripts/modeling",
    "scripts/modeling/check_data_quality",
]

# Create __init__.py in each directory if missing
for dir in dirs:
    init_path = os.path.join(dir, "__init__.py")
    if not os.path.exists(init_path):
        with open(init_path, "w") as f:
            pass  # Just create an empty file
        print(f"✅ Created: {init_path}")
    else:
        print(f"🔹 Already exists: {init_path}")

print("\n🎉 All necessary __init__.py files are in place!")
