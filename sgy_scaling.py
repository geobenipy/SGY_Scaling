#!/usr/bin/env python3
"""
SGY Scaling Tool
----------------
This script normalizes all SEG-Y (.sgy) seismic files in a given input folder
by dividing each trace by the global maximum amplitude across all files.
The original folder structure is preserved in the output folder.
"""

import os
from obspy import read
from tqdm import tqdm

# -------------------------
# CONFIGURATION
# -------------------------
INPUT_FOLDER = r"Path/To/Input"     # Replace with your input folder
OUTPUT_FOLDER = r"Path/To/Output"   # Replace with your output folder

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# -------------------------
# Step 1: Determine global maximum
# -------------------------
global_max = 0
sgy_files = []

for root, dirs, files in os.walk(INPUT_FOLDER):
    for filename in files:
        if filename.lower().endswith('.sgy'):
            sgy_files.append(os.path.join(root, filename))

for filepath in tqdm(sgy_files, desc="Calculating global maximum"):
    stream = read(filepath)
    file_max = max(abs(tr.data).max() for tr in stream)
    global_max = max(global_max, file_max)

print(f"Global maximum amplitude: {global_max}")

# -------------------------
# Step 2: Scale files and save
# -------------------------
for filepath in tqdm(sgy_files, desc="Scaling files"):
    stream = read(filepath)

    # Scale each trace by global maximum
    for tr in stream:
        tr.data = tr.data / global_max

    # Recreate output folder structure
    relative_path = os.path.relpath(os.path.dirname(filepath), INPUT_FOLDER)
    output_dir = os.path.join(OUTPUT_FOLDER, relative_path)
    os.makedirs(output_dir, exist_ok=True)

    # Save scaled SEG-Y file
    output_path = os.path.join(output_dir, os.path.basename(filepath))
    stream.write(output_path, format='SEGY')

print("Scaling complete!")
