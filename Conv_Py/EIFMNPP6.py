# !/usr/bin/env python3
"""
Program: EIFMNPP6
Function: Convert RPS format to CAC format by branch
"""

import re
from pathlib import Path

LRECL_CAC = 138
TEMP_CAC_FILES = {
    '911': 'SAP_PBB_NPL_CAC911.txt',
    '912': 'SAP_PBB_NPL_CAC912.txt',
    '913': 'SAP_PBB_NPL_CAC913.txt',
    '914': 'SAP_PBB_NPL_CAC914.txt',
    '915': 'SAP_PBB_NPL_CAC915.txt',
    '916': 'SAP_PBB_NPL_CAC916.txt'
}

# Placeholder for CACBRCH format mapping
CACBRCH_MAP = {}


def get_cac_from_branch(branch_num):
    """Get CAC code from branch number"""
    return CACBRCH_MAP.get(branch_num, '000')


def process_eifmnpp6(input_file, output_file):
    """
    Convert RPS format to CAC format, splitting by CAC branch code.
    """
    print("=" * 80)
    print("EIFMNPP6 - Converting RPS to CAC Format")
    print("=" * 80)

    if not Path(input_file).exists():
        print(f"Input file {input_file} not found. Creating empty output.")
        Path(output_file).touch()
        for filepath in TEMP_CAC_FILES.values():
            Path(filepath).touch()
        return

    # Open all CAC files
    cac_files = {cac: open(filepath, 'w') for cac, filepath in TEMP_CAC_FILES.items()}

    current_branch = '   '
    current_cac = '000'
    current_cacind = 0

    with open(input_file, 'r') as inf:
        for line in inf:
            line = line.rstrip('\n')

            # Check for branch indicator
            branch_found = False
            if 'BRANCH :' in line[24:33] if len(line) > 33 else False:
                try:
                    branch_str = line[33:36].strip()
                    if branch_str.isdigit():
                        current_branch = branch_str
                        branch_num = int(branch_str)
                        current_cac = get_cac_from_branch(branch_num)
                        current_cacind = 1 if current_cac != '000' else 0
                        branch_found = True

                        if current_cacind == 1 and current_cac in cac_files:
                            f = cac_files[current_cac]
                            f.write('E255'.ljust(LRECL_CAC - 1) + '\n')
                            f.write('P000REPORT NO :  SANPLRPS REPORTS'.ljust(LRECL_CAC - 1) + '\n')
                            f.write(f'P001    {line}'.ljust(LRECL_CAC - 1) + '\n')
                except:
                    pass

            # Write regular lines
            if not branch_found and current_cacind == 1 and current_cac in cac_files:
                f = cac_files[current_cac]
                f.write(f'P001    {line}'.ljust(LRECL_CAC - 1) + '\n')

    # Close all CAC files
    for f in cac_files.values():
        f.close()

    # Combine all CAC files into main output
    with open(output_file, 'w') as fout:
        for cac in sorted(TEMP_CAC_FILES.keys()):
            filepath = TEMP_CAC_FILES[cac]
            if Path(filepath).exists() and Path(filepath).stat().st_size > 0:
                fout.write('E255'.ljust(LRECL_CAC - 1) + '\n')
                fout.write(f'P000PBBEDPPBBEDP{"":97}B{cac}'.ljust(LRECL_CAC - 1) + '\n')
                fout.write('P000REPORT NO :  SANPLRPS REPORTS'.ljust(LRECL_CAC - 1) + '\n')

                with open(filepath, 'r') as fin:
                    lines = fin.readlines()[3:]  # Skip first 3 lines
                    for line in lines:
                        fout.write(line)

    print(f"Generated: {output_file}")
    print("EIFMNPP6 processing complete")


if __name__ == "__main__":
    # Example usage - will be called from EIFMNPLP
    pass
