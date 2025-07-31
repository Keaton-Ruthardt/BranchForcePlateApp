"""
Fix CONCENTRIC_RFD metric name back to correct format
"""

import re

def fix_concentric_rfd():
    # Read the file
    with open('enhanced_cmj_processor.py', 'r') as f:
        content = f.read()
    
    # Fix CONCENTRIC_RFD_Trial_N/s -> CONCENTRIC_RFD_Trial_N_s (back to correct format)
    content = re.sub(r"'CONCENTRIC_RFD_Trial_N/s'", "'CONCENTRIC_RFD_Trial_N_s'", content)
    
    # Write the fixed content back
    with open('enhanced_cmj_processor.py', 'w') as f:
        f.write(content)
    
    print("Fixed CONCENTRIC_RFD metric name back to CONCENTRIC_RFD_Trial_N_s")

if __name__ == "__main__":
    fix_concentric_rfd() 