# SGY Scaling Tool

This Python script normalizes all SEG-Y (.sgy) seismic files in a specified folder 
by dividing each trace by the global maximum amplitude across all files. 
The original folder structure is preserved in the output directory.

## Features

- Automatically finds all `.sgy` files in a folder recursively.
- Computes the global maximum amplitude across all files.
- Scales each trace accordingly.
- Preserves the original folder structure in the output.

## Usage

1. Install dependencies (see `requirements.txt`).
2. Edit the `INPUT_FOLDER` and `OUTPUT_FOLDER` paths in `sgy_scaling.py`.
3. Run the script:

```bash
python sgy_scaling.py
