#!/usr/bin/env python3
"""Fix emoji encoding issues by replacing them with ASCII equivalents"""

import sys

def fix_emojis(filepath):
    # Read file
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Replace emojis line by line
    replacements = {
        '🚀': '[NPU]',
        '✅': '[OK]',
        '🎯': '[ACTION]',
        '📊': '[INFO]',
        '🔍': '[SEARCH]',
        '❌': '[ERROR]',
        '💾': '[SAVE]',
        '⚡': '[PERF]',
        '💻': '[CPU]',
    }
    
    modified = False
    for i, line in enumerate(lines):
        original = line
        for emoji, replacement in replacements.items():
            if emoji in line:
                line = line.replace(emoji, replacement)
                modified = True
        lines[i] = line
    
    # Write back if modified
    if modified:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.writelines(lines)
        print(f"Fixed emojis in {filepath}")
        return True
    else:
        print(f"No emojis found in {filepath}")
        return False

if __name__ == '__main__':
    if len(sys.argv) > 1:
        filepath = sys.argv[1]
        fix_emojis(filepath)
    else:
        # Fix all key files
        files = [
            'src/nba_betting/cli.py',
        ]
        for f in files:
            fix_emojis(f)
