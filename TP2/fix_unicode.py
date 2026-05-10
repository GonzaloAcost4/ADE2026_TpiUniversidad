with open('3-ETL_Incremental/carga_incremental.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace specific problem characters
replacements = {
    '\u2550': '=',
    '\u2192': '->',
    '\u25aa': '*',
}

for old, new in replacements.items():
    content = content.replace(old, new)

# Also remove stray unicode that breaks parsing
import re
# Remove any remaining non-ASCII characters in the file
content = re.sub(r'[^\x00-\x7F]', '', content)

with open('3-ETL_Incremental/carga_incremental.py', 'w', encoding='utf-8') as f:
    f.write(content)

print('Fixed')
