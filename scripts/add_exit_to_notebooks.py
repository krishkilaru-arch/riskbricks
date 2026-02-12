#!/usr/bin/env python3
"""
Add dbutils.notebook.exit("success") to all Databricks notebooks
"""

import os
import sys
from pathlib import Path

def add_exit_cell(notebook_path):
    """Add exit cell to a notebook if it doesn't already have one"""
    
    with open(notebook_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Check if already has exit
    if 'dbutils.notebook.exit' in content:
        return False, "Already has exit"
    
    # Add the exit cell
    exit_cell = '''
# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")
'''
    
    # Append to end of file
    new_content = content.rstrip() + '\n' + exit_cell + '\n'
    
    with open(notebook_path, 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    return True, "Added exit cell"

def main():
    # Find all Python notebooks
    notebooks_dir = Path(__file__).parent.parent / 'notebooks'
    
    if not notebooks_dir.exists():
        print(f"❌ Notebooks directory not found: {notebooks_dir}")
        sys.exit(1)
    
    notebooks = list(notebooks_dir.rglob('*.py'))
    
    print(f"🔍 Found {len(notebooks)} notebooks")
    print("=" * 60)
    
    added = 0
    skipped = 0
    
    for notebook in sorted(notebooks):
        rel_path = notebook.relative_to(notebooks_dir)
        modified, reason = add_exit_cell(notebook)
        
        if modified:
            print(f"✅ {rel_path}: {reason}")
            added += 1
        else:
            print(f"⏭️  {rel_path}: {reason}")
            skipped += 1
    
    print("=" * 60)
    print(f"✅ Added exit cells: {added}")
    print(f"⏭️  Skipped: {skipped}")
    print(f"📊 Total: {len(notebooks)}")

if __name__ == '__main__':
    main()
