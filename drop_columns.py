import pandas as pd
import sys
from pathlib import Path

def drop_columns_from_csv(input_file, output_file=None):
    """
    Drop specified columns from a CSV file.
    
    Args:
        input_file: Path to the input CSV file
        output_file: Path to save the output CSV file. If None, overwrites input file.
    """
    columns_to_drop = [
        "top_game",
        "top_game_hours",
        "top_game_pct",
        "game_count",
        "is_variety_streamer",
        "peak_view_rank",
        "avg_viewer_rank",
        "follower_rank",
        "follower_gain_rank"
    ]
    
    # Validate input file exists
    if not Path(input_file).exists():
        print(f"Error: Input file '{input_file}' does not exist.")
        sys.exit(1)
    
    try:
        # Read the CSV file
        print(f"Reading CSV file: {input_file}")
        df = pd.read_csv(input_file)
        
        # Check which columns exist in the dataframe
        existing_columns = [col for col in columns_to_drop if col in df.columns]
        missing_columns = [col for col in columns_to_drop if col not in df.columns]
        
        if missing_columns:
            print(f"Warning: The following columns do not exist in the CSV: {missing_columns}")
        
        if not existing_columns:
            print("Error: None of the specified columns exist in the CSV file.")
            sys.exit(1)
        
        # Drop the columns that exist
        df_dropped = df.drop(columns=existing_columns)
        
        print(f"Dropped {len(existing_columns)} columns: {existing_columns}")
        
        # Set output file
        if output_file is None:
            output_file = input_file
        
        # Save the file
        df_dropped.to_csv(output_file, index=False)
        print(f"Successfully saved to: {output_file}")
        print(f"Original shape: {df.shape}")
        print(f"New shape: {df_dropped.shape}")
        
    except Exception as e:
        print(f"Error processing CSV file: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python drop_columns.py <input_csv> [output_csv]")
        print("Example: python drop_columns.py data.csv cleaned_data.csv")
        print("If output_csv is omitted, the input file will be overwritten.")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    drop_columns_from_csv(input_file, output_file)
