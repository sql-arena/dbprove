#!/bin/bash

# publish.sh - Publish structured CSV data from run/proof to dbprove-results repo
# Usage: ./scripts/publish.sh <publisher_name>

if [ -z "$1" ]; then
    echo "Usage: $0 <publisher_name>"
    exit 1
fi

PUBLISHER="$1"
RESULTS_REPO="../dbprove-results"
SOURCE_DIR="run/proof"

if [ ! -d "$RESULTS_REPO" ]; then
    echo "Error: Target repository '$RESULTS_REPO' not found in the parent directory."
    exit 1
fi

if [ ! -d "$SOURCE_DIR" ]; then
    echo "Error: Source directory '$SOURCE_DIR' not found."
    exit 1
fi

echo "Publishing as '$PUBLISHER'..."

# Iterate through engine folders in run/proof
for engine_path in "$SOURCE_DIR"/*; do
    [ -d "$engine_path" ] || continue
    engine_name=$(basename "$engine_path")
    
    echo "Processing engine: $engine_name"
    
    # Iterate through version folders
    for version_path in "$engine_path"/*; do
        [ -d "$version_path" ] || continue
        version=$(basename "$version_path")
        
        echo "  Version: $version"
        
        # Target engine directory in the results repo
        engine_dir="$RESULTS_REPO/engine/$engine_name"
        if [ ! -d "$engine_dir" ]; then
            # Try case-insensitive match
            matched_dir=$(find "$RESULTS_REPO/engine" -maxdepth 1 -iname "$engine_name" -type d | head -n 1)
            if [ -n "$matched_dir" ]; then
                engine_dir="$matched_dir"
            fi
        fi
        
        if [ ! -d "$engine_dir" ]; then
            echo "    Warning: Engine directory '$engine_name' not found in $RESULTS_REPO/engine/. Skipping."
            continue
        fi
        
        # Target version directory in the results repo
        target_version_dir="$engine_dir/$version"
        if [ ! -d "$target_version_dir" ]; then
             # Try a partial match for the version
             matched_ver=$(ls -1 "$engine_dir" | grep -v "README.md" | grep -v "submission.md" | grep "$version" | head -n 1)
             if [ -n "$matched_ver" ]; then
                 target_version_dir="$engine_dir/$matched_ver"
                 echo "    Matched version $version to $matched_ver"
             else
                 echo "    Warning: Version folder matching '$version' not found in $engine_dir. Skipping."
                 continue
             fi
        fi
        
        # Destination directory: engine/<version>/<publisher>/
        dest_dir="$target_version_dir/$PUBLISHER"
        echo "    Destination: $dest_dir"
        
        # Create the publisher directory if it doesn't exist
        mkdir -p "$dest_dir"
        
        # Copy everything from the version folder (should be the CSV files)
        cp -R "$version_path"/* "$dest_dir/"
        echo "    Copied data to $dest_dir/"
    done
done

echo "Done."
