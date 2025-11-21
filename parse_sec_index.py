#!/usr/bin/env python3
"""
Parse SEC Master Index .idx file and filter for specific form types.
Converts pipe-delimited format to CSV and filters for Forms 4, 13D, 13D/A, 13G, 13G/A.
"""

import pandas as pd
import sys
import os


def parse_sec_index(input_file: str, output_file: str = "master_index_filtered.csv"):
    """
    Parse SEC Master Index file and filter for specific form types.
    
    Args:
        input_file: Path to the .idx file (pipe-delimited format)
        output_file: Path to output CSV file
    """
    # Form types to keep (including both with and without "SC " prefix)
    form_types_to_keep = ['4', '13D', '13D/A', '13G', '13G/A', 
                          'SC 13D', 'SC 13D/A', 'SC 13G', 'SC 13G/A']
    
    print(f"Reading SEC Master Index file: {input_file}")
    
    # Read the file line by line to skip header
    data_lines = []
    header_found = False
    column_line = None
    
    with open(input_file, 'r', encoding='utf-8', errors='ignore') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            
            # Skip empty lines
            if not line:
                continue
            
            # Look for the column header line
            if 'CIK|Company Name|Form Type|Date Filed|Filename' in line:
                column_line = line
                header_found = True
                print(f"Found header at line {line_num}")
                continue
            
            # Skip lines before header (description, comments, etc.)
            if not header_found:
                continue
            
            # Skip the separator line (dashes)
            if line.startswith('---'):
                continue
            
            # Parse data lines (pipe-delimited)
            if '|' in line:
                data_lines.append(line)
    
    if not header_found:
        print("Error: Could not find column header in the file")
        sys.exit(1)
    
    print(f"Found {len(data_lines)} data rows")
    
    # Parse into DataFrame
    # Split by pipe and create DataFrame
    rows = []
    for line in data_lines:
        parts = line.split('|')
        if len(parts) >= 5:
            rows.append({
                'CIK': parts[0].strip(),
                'Company Name': parts[1].strip(),
                'Form Type': parts[2].strip(),
                'Date Filed': parts[3].strip(),
                'Filename': parts[4].strip()
            })
    
    df = pd.DataFrame(rows)
    
    if df.empty:
        print("Warning: No data rows found after parsing")
        return
    
    print(f"Parsed {len(df)} rows into DataFrame")
    print(f"Form types in data: {df['Form Type'].unique()[:10]}")  # Show first 10 unique forms
    
    # Filter for specified form types
    print(f"\nFiltering for forms: {form_types_to_keep}")
    filtered_df = df[df['Form Type'].isin(form_types_to_keep)]
    
    print(f"Found {len(filtered_df)} rows matching the specified forms")
    
    # Show breakdown by form type
    if not filtered_df.empty:
        print("\nBreakdown by form type:")
        print(filtered_df['Form Type'].value_counts())
    
    # Save to CSV
    filtered_df.to_csv(output_file, index=False)
    print(f"\nFiltered data saved to: {output_file}")
    
    return filtered_df


if __name__ == "__main__":
    input_file = "Master Index.idx.backup"
    
    if not os.path.exists(input_file):
        print(f"Error: File '{input_file}' not found")
        sys.exit(1)
    
    output_file = "master_index_filtered.csv"
    parse_sec_index(input_file, output_file)

