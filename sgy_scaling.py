#!/usr/bin/env python3
"""
SGY Scaling Tool - Ultra Fast Multicore Version
------------------------------------------------
This script normalizes all SEG-Y (.sgy) seismic files in a given input folder
by dividing each trace by the global maximum amplitude across all files.
Uses multiprocessing for maximum speed.
"""
import os
from obspy import read
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
import numpy as np

# -------------------------
# CONFIGURATION
# -------------------------
INPUT_FOLDER = r"D:\Haimerl\PhD\ML\Data_Unscaled"
OUTPUT_FOLDER = r"D:\Haimerl\PhD\ML\Data_Scaled"
NUM_CORES = max(1, int(cpu_count() * 0.01)) 

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# -------------------------
# Helper Functions
# -------------------------
def find_file_max(filepath):
    """Findet Maximum in einer einzelnen Datei"""
    try:
        stream = read(filepath, format='SEGY')
        file_max = max(abs(tr.data).max() for tr in stream)
        return file_max
    except Exception as e:
        print(f"\nError reading {filepath}: {e}")
        return 0

def scale_and_save(args):
    """Skaliert und speichert eine einzelne Datei"""
    filepath, global_max, input_folder, output_folder = args
    try:
        stream = read(filepath, format='SEGY')
        
        # Scale each trace by global maximum
        for tr in stream:
            tr.data = tr.data / global_max
        
        # Recreate output folder structure
        relative_path = os.path.relpath(os.path.dirname(filepath), input_folder)
        output_dir = os.path.join(output_folder, relative_path)
        os.makedirs(output_dir, exist_ok=True)
        
        # Save scaled SEG-Y file
        output_path = os.path.join(output_dir, os.path.basename(filepath))
        stream.write(output_path, format='SEGY')
        return True
    except Exception as e:
        print(f"\nError processing {filepath}: {e}")
        return False

# -------------------------
# Main Processing
# -------------------------
if __name__ == '__main__':
    # Collect all SGY files
    sgy_files = []
    for root, dirs, files in os.walk(INPUT_FOLDER):
        for filename in files:
            if filename.lower().endswith('.sgy'):
                sgy_files.append(os.path.join(root, filename))
    
    print(f"Found {len(sgy_files)} SEG-Y files")
    print(f"Using {NUM_CORES} CPU cores")
    
    # -------------------------
    # Step 1: Find global maximum (parallel)
    # -------------------------
    print("\nStep 1: Finding global maximum...")
    with Pool(NUM_CORES) as pool:
        maxima = list(tqdm(
            pool.imap(find_file_max, sgy_files),
            total=len(sgy_files),
            desc="Calculating global max"
        ))
    
    global_max = max(maxima)
    print(f"\nGlobal maximum amplitude: {global_max}")
    
    if global_max == 0:
        print("Error: Global maximum is 0. Cannot scale files.")
        exit(1)
    
    # -------------------------
    # Step 2: Scale and save (parallel)
    # -------------------------
    print("\nStep 2: Scaling and saving files...")
    args_list = [(f, global_max, INPUT_FOLDER, OUTPUT_FOLDER) for f in sgy_files]
    
    with Pool(NUM_CORES) as pool:
        results = list(tqdm(
            pool.imap(scale_and_save, args_list),
            total=len(args_list),
            desc="Scaling files"
        ))
    
    successful = sum(results)
    print(f"\nScaling complete! Successfully processed {successful}/{len(sgy_files)} files")