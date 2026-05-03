#!/bin/bash

# Install dependencies
npm install

# Initialize database
rm -f podcasts.db

# Ensure data directory exists
mkdir -p data

echo "Installation complete. Database initialized and data directory ready."
