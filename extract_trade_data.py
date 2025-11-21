#!/usr/bin/env python3
"""
Download SEC filings and extract trade/ownership information.
Updates the CSV with extracted data including shares, prices, transaction types, etc.
"""

import requests
import pandas as pd
from pathlib import Path
import time
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import re
from typing import Dict, List, Optional
import sys


class SECFilingParser:
    """Parse SEC filings to extract trade and ownership information."""
    
    def __init__(self):
        self.namespaces = {
            'edgar': 'http://www.sec.gov/edgar/document/edgardocument',
            'ed': 'http://www.sec.gov/edgar/document/edgardocument',
        }
    
    def parse_form4(self, content: str) -> Dict:
        """Parse Form 4 (XML) to extract transaction details.
        
        Form 4 files contain XML embedded within <XML>...</XML> tags.
        """
        result = {
            'Transaction_Code': None,
            'Transaction_Date': None,
            'Shares_Acquired': None,
            'Shares_Disposed': None,
            'Price_Per_Share': None,
            'Security_Name': None,
            'Transaction_Type': None,
            'Total_Value': None,
            'Shares_Owned_Following': None,
        }
        
        try:
            # Extract XML content from <XML>...</XML> tags
            xml_start = content.find('<XML>')
            xml_end = content.find('</XML>')
            
            if xml_start == -1 or xml_end == -1:
                return result
            
            xml_content = content[xml_start + 5:xml_end].strip()
            
            # Parse the XML
            root = ET.fromstring(xml_content)
            
            # Find all nonDerivativeTransactions
            transactions = root.findall('.//nonDerivativeTransaction')
            
            if transactions:
                # Get first transaction (most recent)
                trans = transactions[0]
                
                # Transaction code
                code_elem = trans.find('.//transactionCode')
                if code_elem is not None:
                    result['Transaction_Code'] = code_elem.text
                    # Map codes to transaction types
                    code_map = {
                        'P': 'Purchase',
                        'S': 'Sale',
                        'A': 'Grant/Award',
                        'D': 'Disposition',
                        'F': 'Payment of exercise price',
                        'I': 'Discretionary transaction',
                        'M': 'Exercise or conversion',
                        'C': 'Conversion',
                        'E': 'Expiration of short derivative position',
                        'H': 'Expiration of long derivative position',
                        'O': 'Transfer Out',
                        'X': 'Exercise of out-of-the-money derivative',
                        'G': 'Bona fide gift',
                        'W': 'Acquisition or disposition by will',
                        'L': 'Small acquisition',
                        'Z': 'Deposit into or withdrawal from voting trust',
                    }
                    result['Transaction_Type'] = code_map.get(result['Transaction_Code'], result['Transaction_Code'])
                
                # Transaction date
                date_elem = trans.find('.//transactionDate')
                if date_elem is not None:
                    date_val = date_elem.find('.//value')
                    if date_val is not None and date_val.text:
                        result['Transaction_Date'] = date_val.text
                
                # Shares
                shares_elem = trans.find('.//transactionShares')
                if shares_elem is not None:
                    shares_val = shares_elem.find('.//value')
                    if shares_val is not None and shares_val.text:
                        try:
                            shares = float(shares_val.text.replace(',', ''))
                            # Determine if acquired or disposed based on transactionAcquiredDisposedCode
                            acquired_disposed_elem = trans.find('.//transactionAcquiredDisposedCode')
                            if acquired_disposed_elem is not None:
                                ad_code_elem = acquired_disposed_elem.find('.//value')
                                if ad_code_elem is not None:
                                    ad_code = ad_code_elem.text
                                    if ad_code == 'A':
                                        result['Shares_Acquired'] = shares
                                    elif ad_code == 'D':
                                        result['Shares_Disposed'] = shares
                            # Fallback: use transaction code
                            elif result['Transaction_Code']:
                                if result['Transaction_Code'] in ['P', 'A', 'M', 'C', 'I', 'L']:
                                    result['Shares_Acquired'] = shares
                                elif result['Transaction_Code'] in ['S', 'D', 'F', 'O']:
                                    result['Shares_Disposed'] = shares
                        except (ValueError, AttributeError):
                            pass
                
                # Price per share
                price_elem = trans.find('.//transactionPricePerShare')
                if price_elem is not None:
                    price_val = price_elem.find('.//value')
                    if price_val is not None and price_val.text:
                        result['Price_Per_Share'] = price_val.text
                
                # Shares owned following transaction
                post_trans_elem = trans.find('.//postTransactionAmounts')
                if post_trans_elem is not None:
                    shares_owned_elem = post_trans_elem.find('.//sharesOwnedFollowingTransaction')
                    if shares_owned_elem is not None:
                        shares_owned_val = shares_owned_elem.find('.//value')
                        if shares_owned_val is not None and shares_owned_val.text:
                            result['Shares_Owned_Following'] = shares_owned_val.text
                
                # Security name
                security_elem = trans.find('.//securityTitle')
                if security_elem is not None:
                    sec_val = security_elem.find('.//value')
                    if sec_val is not None and sec_val.text:
                        result['Security_Name'] = sec_val.text
                
                # Calculate total value if we have shares and price
                if result['Shares_Acquired'] and result['Price_Per_Share']:
                    try:
                        price = float(result['Price_Per_Share'])
                        result['Total_Value'] = result['Shares_Acquired'] * price
                    except (ValueError, TypeError):
                        pass
                elif result['Shares_Disposed'] and result['Price_Per_Share']:
                    try:
                        price = float(result['Price_Per_Share'])
                        result['Total_Value'] = result['Shares_Disposed'] * price
                    except (ValueError, TypeError):
                        pass
            
        except ET.ParseError as e:
            # XML parsing failed
            pass
        except Exception as e:
            print(f"  Warning: Error parsing Form 4: {e}")
        
        return result
    
    def parse_13g_13d(self, content: str) -> Dict:
        """Parse 13G/13D forms (HTML) to extract ownership information.
        
        These forms use HTML tables with numbered rows:
        - Row 11 (13D) or Row 9 (13G): Aggregate Amount Beneficially Owned
        - Row 13 (13D) or Row 11 (13G): Percent of Class
        - Row 7: Sole Voting Power
        - Row 8: Shared Voting Power
        - Row 9: Sole Dispositive Power
        - Row 10: Shared Dispositive Power
        """
        result = {
            'Shares_Owned': None,
            'Percent_of_Class': None,
            'Sole_Voting_Power': None,
            'Shared_Voting_Power': None,
            'Sole_Dispositive_Power': None,
            'Shared_Dispositive_Power': None,
            'CUSIP': None,
            'Security_Name': None,
            'Type_of_Reporting_Person': None,
        }
        
        try:
            # Extract HTML content if embedded in <TEXT> tags
            text_start = content.find('<TEXT>')
            text_end = content.find('</TEXT>')
            if text_start != -1 and text_end != -1:
                html_content = content[text_start + 6:text_end]
            else:
                html_content = content
            
            soup = BeautifulSoup(html_content, 'lxml')
            
            # Get text content for regex searches
            text = soup.get_text()
            
            # Extract CUSIP number
            # Look for "CUSIP" followed by a number
            cusip_pattern = re.compile(r'CUSIP[^\d]*(\d{6,9})', re.IGNORECASE)
            cusip_match = cusip_pattern.search(text)
            if cusip_match:
                result['CUSIP'] = cusip_match.group(1)
            
            # Extract Security Name (Title of Class of Securities)
            security_name_pattern = re.compile(r'\(Title of Class of Securities\)\s*([^\n(]+)', re.IGNORECASE)
            security_match = security_name_pattern.search(text)
            if security_match:
                result['Security_Name'] = security_match.group(1).strip()
            
            # Find all tables
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) < 2:
                        continue
                    
                    # Look for row numbers (7, 8, 9, 10, 11, 13)
                    first_cell_text = cells[0].get_text().strip()
                    
                    # Extract row number
                    row_num_match = re.search(r'^(\d+)\.', first_cell_text)
                    if not row_num_match:
                        continue
                    
                    row_num = int(row_num_match.group(1))
                    row_text = ' '.join([cell.get_text() for cell in cells])
                    
                    # Row 7: Sole Voting Power
                    if row_num == 7 and 'Sole Voting Power' in row_text:
                        value_text = cells[-1].get_text().strip()
                        # Extract number (may have commas and footnotes)
                        num_match = re.search(r'([\d,]+)', value_text.replace('-0-', '0'))
                        if num_match:
                            result['Sole_Voting_Power'] = num_match.group(1).replace(',', '')
                    
                    # Row 8: Shared Voting Power
                    elif row_num == 8 and 'Shared Voting Power' in row_text:
                        value_text = cells[-1].get_text().strip()
                        num_match = re.search(r'([\d,]+)', value_text.replace('-0-', '0'))
                        if num_match:
                            result['Shared_Voting_Power'] = num_match.group(1).replace(',', '')
                    
                    # Row 9: Sole Dispositive Power (or Aggregate Amount for 13G)
                    elif row_num == 9:
                        if 'Sole Dispositive Power' in row_text:
                            value_text = cells[-1].get_text().strip()
                            num_match = re.search(r'([\d,]+)', value_text.replace('-0-', '0'))
                            if num_match:
                                result['Sole_Dispositive_Power'] = num_match.group(1).replace(',', '')
                        elif 'Aggregate Amount Beneficially Owned' in row_text:
                            # This is for 13G forms
                            value_text = cells[-1].get_text().strip()
                            num_match = re.search(r'([\d,]+)', value_text.replace('-0-', '0'))
                            if num_match:
                                result['Shares_Owned'] = num_match.group(1).replace(',', '')
                    
                    # Row 10: Shared Dispositive Power
                    elif row_num == 10 and 'Shared Dispositive Power' in row_text:
                        value_text = cells[-1].get_text().strip()
                        num_match = re.search(r'([\d,]+)', value_text.replace('-0-', '0'))
                        if num_match:
                            result['Shared_Dispositive_Power'] = num_match.group(1).replace(',', '')
                    
                    # Row 11: Aggregate Amount Beneficially Owned (for 13D)
                    elif row_num == 11 and 'Aggregate Amount Beneficially Owned' in row_text:
                        value_text = cells[-1].get_text().strip()
                        num_match = re.search(r'([\d,]+)', value_text.replace('-0-', '0'))
                        if num_match:
                            result['Shares_Owned'] = num_match.group(1).replace(',', '')
                    
                    # Row 11: Percent of Class (for 13G)
                    elif row_num == 11 and 'Percent of Class' in row_text:
                        value_text = cells[-1].get_text().strip()
                        # Extract percentage (may be in format like "6.1%" or "7.94%")
                        percent_match = re.search(r'(\d+\.?\d*)\s*%', value_text)
                        if percent_match:
                            result['Percent_of_Class'] = percent_match.group(1)
                    
                    # Row 13: Percent of Class (for 13D)
                    elif row_num == 13 and 'Percent of Class' in row_text:
                        value_text = cells[-1].get_text().strip()
                        percent_match = re.search(r'(\d+\.?\d*)\s*%', value_text)
                        if percent_match:
                            result['Percent_of_Class'] = percent_match.group(1)
                    
                    # Row 12: Type of Reporting Person
                    elif row_num == 12 and 'Type of Reporting Person' in row_text:
                        value_text = cells[-1].get_text().strip()
                        # Common types: IN (Individual), IA (Investment Adviser), CO (Company), etc.
                        type_match = re.search(r'\b([A-Z]{2,3})\b', value_text)
                        if type_match:
                            result['Type_of_Reporting_Person'] = type_match.group(1)
            
            # Fallback: try regex patterns if table parsing didn't work
            if not result['Shares_Owned']:
                # Look for "Aggregate Amount Beneficially Owned"
                agg_match = re.search(r'Aggregate Amount Beneficially Owned[^\d]*([\d,]+)', text, re.IGNORECASE)
                if agg_match:
                    result['Shares_Owned'] = agg_match.group(1).replace(',', '')
            
            if not result['Percent_of_Class']:
                # Look for "Percent of Class"
                percent_match = re.search(r'Percent of Class[^\d]*(\d+\.?\d*)\s*%', text, re.IGNORECASE)
                if percent_match:
                    result['Percent_of_Class'] = percent_match.group(1)
                
        except Exception as e:
            print(f"  Warning: Error parsing 13G/13D HTML: {e}")
        
        return result


def download_and_extract_trades(
    csv_file: str,
    output_csv: str = None,
    user_agent: str = "Shiven Shekar shiven.shekar@asu.edu",
    request_delay: float = 0.11
):
    """
    Download SEC filings and extract trade information.
    
    Args:
        csv_file: Path to input CSV with filings
        output_csv: Path to output CSV (default: updates input file)
        user_agent: User-Agent string (required by SEC)
        request_delay: Delay between requests in seconds (default: 0.11 for ~9 req/sec)
    """
    # Read the CSV
    print(f"Reading {csv_file}...")
    df = pd.read_csv(csv_file)
    print(f"Found {len(df)} filings to process")
    
    # Initialize parser
    parser = SECFilingParser()
    
    # Set up headers
    headers = {
        'User-Agent': user_agent,
        'Accept-Encoding': 'gzip, deflate',
        'Host': 'www.sec.gov',
    }
    
    # Initialize columns for extracted data
    if 'Form Type' in df.columns:
        form4_cols = ['Transaction_Code', 'Transaction_Date', 'Shares_Acquired', 
                     'Shares_Disposed', 'Price_Per_Share', 'Security_Name', 
                     'Transaction_Type', 'Total_Value', 'Shares_Owned_Following']
        form13_cols = ['Shares_Owned', 'Percent_of_Class', 'Sole_Voting_Power',
                      'Shared_Voting_Power', 'Sole_Dispositive_Power', 
                      'Shared_Dispositive_Power', 'CUSIP', 'Security_Name',
                      'Type_of_Reporting_Person']
        
        # Combine and deduplicate (Security_Name appears in both)
        all_cols = list(set(form4_cols + form13_cols))
        for col in all_cols:
            if col not in df.columns:
                df[col] = None
    
    print(f"\nDownloading and parsing filings...")
    print(f"Rate limit: ~{1/request_delay:.1f} requests/second")
    print("-" * 60)
    
    errors = []
    
    for idx, row in df.iterrows():
        try:
            # Build URL
            filename = row['Filename']
            url = f"https://www.sec.gov/Archives/{filename}"
            
            form_type = row.get('Form Type', '')
            
            # Progress update
            if (idx + 1) % 10 == 0:
                print(f"Processing {idx + 1}/{len(df)}... ({form_type})")
            
            # Download with rate limiting
            time.sleep(request_delay)
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            content = response.text
            
            # Parse based on form type
            if form_type == '4':
                # Form 4 - extract transaction details
                parsed = parser.parse_form4(content)
                for key, value in parsed.items():
                    if key in df.columns:
                        df.at[idx, key] = value
            elif form_type in ['13G', '13G/A', '13D', '13D/A', 'SC 13G', 'SC 13G/A', 'SC 13D', 'SC 13D/A']:
                # 13G/13D - extract ownership information
                parsed = parser.parse_13g_13d(content)
                for key, value in parsed.items():
                    if key in df.columns:
                        df.at[idx, key] = value
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Error downloading {row.get('Filename', 'unknown')}: {e}"
            print(f"  {error_msg}")
            errors.append(error_msg)
            continue
        except Exception as e:
            error_msg = f"Error processing row {idx}: {e}"
            print(f"  {error_msg}")
            errors.append(error_msg)
            continue
    
    # Save updated CSV
    output_file = output_csv if output_csv else csv_file
    df.to_csv(output_file, index=False)
    print(f"\n{'='*60}")
    print(f"Completed! Updated data saved to: {output_file}")
    print(f"Successfully processed: {len(df) - len(errors)}/{len(df)} filings")
    if errors:
        print(f"Errors encountered: {len(errors)}")
    
    return df


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Download SEC filings and extract trade data')
    parser.add_argument('--input', '-i', default='master_index_filtered.csv',
                       help='Input CSV file (default: master_index_filtered.csv)')
    parser.add_argument('--output', '-o', default=None,
                       help='Output CSV file (default: updates input file)')
    parser.add_argument('--user-agent', '-u', default='Shiven Shekar shiven.shekar@asu.edu',
                       help='User-Agent string (required by SEC)')
    parser.add_argument('--delay', '-d', type=float, default=0.11,
                       help='Delay between requests in seconds (default: 0.11)')
    
    args = parser.parse_args()
    
    download_and_extract_trades(
        csv_file=args.input,
        output_csv=args.output,
        user_agent=args.user_agent,
        request_delay=args.delay
    )

