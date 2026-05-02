#!/bin/bash

# Install dependencies
npm install

# Initialize database
# We remove the existing one if it exists to ensure a clean start for new environments
rm -f podcasts.db

echo "Installation complete. Database initialized."
