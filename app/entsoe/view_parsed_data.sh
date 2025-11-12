#!/bin/bash
# View parsed ENTSOE data as markdown table

PERIOD_START=${1:-202511100800}
PERIOD_END=${2:-202511101200}

OUTPUT_FILE="/app/scripts/entsoe/data/parsed_data_${PERIOD_START}_${PERIOD_END}.md"

echo "Parsing and saving to: $OUTPUT_FILE"

python3 /app/scripts/entsoe/parse_imbalance_to_db.py --period $PERIOD_START $PERIOD_END > "$OUTPUT_FILE" 2>&1

echo ""
echo "Output saved to: $OUTPUT_FILE"
echo ""
echo "=== MARKDOWN TABLE ==="
cat "$OUTPUT_FILE"
