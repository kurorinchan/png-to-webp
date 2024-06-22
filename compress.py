#!/usr/bin/env python3

import argparse
import os
from pathlib import Path
import subprocess
from concurrent.futures import Future, ProcessPoolExecutor
import sys
from typing import Dict, List

_COMPRESSED_FILE_TYPE = "webp"


# Define a function to run the 'magick' program on a file
def run_magick(file: Path, output_dir: Path) -> str:
    """
    Run the 'magick' program on a single PNG image file.

    Args:
        file (Path): The path to the input PNG image file.
        output_dir (Path): The directory where the output compressed file will be written.

    Returns:
        str: The name of the file that was processed.
    """
    output_webp_file = output_dir / (file.stem + "." + _COMPRESSED_FILE_TYPE)

    # This could be done with mogrify command. However I sometimes see it fail to preserver the timestamp.
    # So instead, the command just converts it and later the timestamps are updated.
    command = ["magick", "-quality", "75", str(file), str(output_webp_file)]

    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode != 0:
        raise Exception(f"'magick' failed on file {file} with error:\\n{result.stderr}")

    file_stat = file.stat()
    os.utime(output_webp_file, (file_stat.st_atime, file_stat.st_mtime))
    return str(file)


def process_images(
    imgs_dir: Path, output_dir: Path, remove_input_on_success: bool
) -> bool:
    """
    Process all PNG files in the given directory in parallel using the 'magick' program.

    Args:
        directory (Path): The directory containing PNG files.

    Returns:
        True on success, False otherwise.
    """
    # Get a list of all PNG files in the directory
    png_files = [imgs_dir / f for f in os.listdir(imgs_dir) if f.endswith(".png")]

    # Create a ProcessPoolExecutor
    with ProcessPoolExecutor() as executor:
        # Run the 'magick' program on all PNG files in parallel
        futures: Dict[Future[str], str] = {
            executor.submit(run_magick, file, output_dir): str(file)
            for file in png_files
        }

        # Wait for all futures to complete and check for errors
        for future in futures:
            try:
                # If the future completed without raising an exception, print the name of the file that was processed
                print(f"Processed file: {future.result()}")
            except Exception as e:
                # If the future raised an exception, print the error message
                print(f"Error processing file {futures[future]}: {e}")
                return False

    for single_file in png_files:
        expected_file = output_dir / (single_file.stem + "." + _COMPRESSED_FILE_TYPE)
        if expected_file.exists():
            if remove_input_on_success:
                os.remove(single_file)
        else:
            print(
                f"Error: Expected file '{expected_file}' not found. Keeping original file."
            )

    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help="Input directory containing PNG files.",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help=f"Output directory for {_COMPRESSED_FILE_TYPE} files.",
    )

    parser.add_argument(
        "--remove_input",
        action="store_true",
        default=False,
        help="Remove input files after successful processing.",
    )

    args = parser.parse_args()

    if not args.input.is_dir():
        print(f"Error: '{args.input}' is not a directory.")
        return

    if not args.output.is_dir():
        print(f"Error: '{args.output}' is not a directory.")
        return

    result = process_images(args.input, args.output, args.remove_input)
    if not result:
        print(f"Operation failed. See above for errors.")
        sys.exit(1)

    print("Operation completed successfully.")


if __name__ == "__main__":
    main()
