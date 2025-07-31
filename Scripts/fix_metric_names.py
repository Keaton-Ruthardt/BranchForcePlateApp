"""
Fix metric names in enhanced_cmj_processor.py to match VALD API
"""

import re

def fix_metric_names():
    # Read the file
    with open('enhanced_cmj_processor.py', 'r') as f:
        content = f.read()
    
    # Fix CON_P2_CON_P1_IMPULSE_RATIO_Trial_ -> CON_P2_CON_P1_IMPULSE_RATIO_Trial
    content = re.sub(r"'CON_P2_CON_P1_IMPULSE_RATIO_Trial_'", "'CON_P2_CON_P1_IMPULSE_RATIO_Trial'", content)
    
    # Fix CONCENTRIC_RFD_Trial_N/s -> CONCENTRIC_RFD_Trial_N_s (in cmj_metrics lists)
    # The API actually returns CONCENTRIC_RFD_Trial_N_s (with underscore)
    content = re.sub(r"'CONCENTRIC_RFD_Trial_N/s'", "'CONCENTRIC_RFD_Trial_N_s'", content)
    
    # Add the missing metric mapping
    content = re.sub(
        r"'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W/kg': 'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg',",
        "'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W/kg': 'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg',\n        'CONCENTRIC_RFD_Trial_N/s': 'CONCENTRIC_RFD_Trial_N_s',",
        content
    )
    
    # Write the fixed content back
    with open('enhanced_cmj_processor.py', 'w') as f:
        f.write(content)
    
    print("Fixed metric names in enhanced_cmj_processor.py")

if __name__ == "__main__":
    fix_metric_names() 