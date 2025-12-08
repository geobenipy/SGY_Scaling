#!/usr/bin/env python3
"""
SGY Scaling Tool - Robust Version with Header Fix
------------------------------------------------
This script normalizes all SEG-Y (.sgy) seismic files in a given input folder
by dividing each trace by the global maximum amplitude across all files.
Fixes header overflow issues when writing by using segyio.
"""
import os
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
import numpy as np
import warnings
import shutil

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

# -------------------------
# CONFIGURATION
# -------------------------
INPUT_FOLDER = r"D:\Haimerl\PhD\ML\Data_Unscaled"
OUTPUT_FOLDER = r"L:\win\AG_Marine_Geophysics\beni\machine-learning\beni"
ERROR_LOG = os.path.join(OUTPUT_FOLDER, "error_log.txt")
NUM_CORES = max(1, int(cpu_count() * 0.85))   # change number to use more cores

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# -------------------------
# Helper Functions
# -------------------------
def find_file_max(filepath):
    """Findet Maximum in einer einzelnen Datei"""
    try:
        import segyio
        with segyio.open(filepath, 'r', ignore_geometry=True) as f:
            maxima = []
            for trace in f.trace:
                if len(trace) > 0:
                    trace_max = np.nanmax(np.abs(trace))
                    if np.isfinite(trace_max) and trace_max > 0:
                        maxima.append(trace_max)
            return max(maxima) if maxima else 0
    except Exception as e:
        print(f"\nError reading {os.path.basename(filepath)}: {str(e)[:100]}")
        return 0

def scale_and_save(args):
    """Skaliert und speichert eine einzelne Datei - using segyio to avoid header issues"""
    filepath, global_max, input_folder, output_folder = args
    
    try:
        import segyio
        
        # Recreate output folder structure
        relative_path = os.path.relpath(os.path.dirname(filepath), input_folder)
        output_dir = os.path.join(output_folder, relative_path)
        os.makedirs(output_dir, exist_ok=True)
        
        # Output path
        output_path = os.path.join(output_dir, os.path.basename(filepath))
        
        # Copy original file first to preserve all headers
        shutil.copy2(filepath, output_path)
        
        # Open copied file and modify traces in-place
        with segyio.open(output_path, 'r+', ignore_geometry=True) as f:
            valid_traces = 0
            for i in range(len(f.trace)):
                trace = f.trace[i]
                if len(trace) > 0:
                    # Check for valid data
                    if not np.all(np.isnan(trace)) and not np.all(np.isinf(trace)):
                        # Scale by global maximum
                        scaled = trace / global_max
                        # Handle any resulting NaN/Inf
                        scaled = np.nan_to_num(scaled, nan=0.0, posinf=0.0, neginf=0.0)
                        # Write back
                        f.trace[i] = scaled.astype(np.float32)
                        valid_traces += 1
        
        if valid_traces == 0:
            return (False, filepath, "No valid traces to scale")
        
        return (True, filepath, f"Success ({valid_traces} traces)")
        
    except ImportError:
        return (False, filepath, "segyio library not installed - run: pip install segyio")
    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)[:200]}"
        return (False, filepath, error_msg)

# -------------------------
# Main Processing
# -------------------------
if __name__ == '__main__':
    # Check if segyio is installed
    try:
        import segyio
    except ImportError:
        print("ERROR: segyio library is required but not installed.")
        print("Please install it using: pip install segyio")
        exit(1)
    
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
    
    # Filter out zero/invalid maxima
    valid_maxima = [m for m in maxima if m > 0 and np.isfinite(m)]
    
    if not valid_maxima:
        print("Error: No valid data found in any files. Cannot scale.")
        exit(1)
    
    global_max = max(valid_maxima)
    print(f"\nGlobal maximum amplitude: {global_max}")
    print(f"Valid files for max calculation: {len(valid_maxima)}/{len(sgy_files)}")
    
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
    
    # Analyze results
    successful = sum(1 for r in results if r[0])
    failed = [r for r in results if not r[0]]
    
    print(f"\n{'='*60}")
    print(f"Scaling complete!")
    print(f"Successfully processed: {successful}/{len(sgy_files)} files")
    print(f"Failed: {len(failed)} files")
    
    # Write error log
    if failed:
        print(f"\nWriting error log to: {ERROR_LOG}")
        with open(ERROR_LOG, 'w', encoding='utf-8') as f:
            f.write(f"SGY Scaling Error Log\n")
            f.write(f"{'='*60}\n")
            f.write(f"Total files: {len(sgy_files)}\n")
            f.write(f"Successful: {successful}\n")
            f.write(f"Failed: {len(failed)}\n\n")
            f.write(f"Failed Files:\n")
            f.write(f"{'-'*60}\n")
            for success, filepath, error in failed:
                f.write(f"\nFile: {os.path.basename(filepath)}\n")
                f.write(f"Path: {filepath}\n")
                f.write(f"Error: {error}\n")
        print(f"See {ERROR_LOG} for details on failed files")
    else:
        print("\nAll files processed successfully!")
    
    print(f"{'='*60}")