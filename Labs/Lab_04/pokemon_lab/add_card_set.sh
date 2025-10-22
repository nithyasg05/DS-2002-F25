#!/usr/bin/env bash

read -p "Enter the TCG Card Set ID (e.g., base1, base4): " SET_ID

# Check if input is empty
if [ -z "$SET_ID" ]; then
    echo "Error: Set ID cannot be empty." >&2
    exit 1
fi

echo "Fetching data for set ID: $SET_ID ..."

curl -s "https://api.pokemontcg.io/v2/cards?q=set.id:${SET_ID}" -o "card_set_lookup/${SET_ID}.json"

echo "Data saved to card_set_lookup/${SET_ID}.json"
