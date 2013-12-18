#!/bin/bash
#
# This script adds the specified main.ipynb file to the given repository.

set -e -x

echo "Content-type: text/plain"
echo

# Prepare scripts.
TEMP_DIRECTORY="$(mktemp -d)"
NONCE="$(date +%s)"
REPO="git@localhost:assignment-one.git"
BRANCH="$QUERY_STRING"

cd $TEMP_DIRECTORY

# Fetch existing code.
git clone "$REPO" -b "$BRANCH" . 0</dev/null

# Ensure that commits are made with the correct author.
git config --local user.name "Cylon Jeremy"
git config --local user.email open-source@fatlotus.com

# Add the modified main.ipynb file back in.
cat > ./main.ipynb
git add ./main.ipynb
git commit --allow-empty -m "AUTO: Add output of main.ipynb to repository."

# Push the results back to Git.
git tag -a "responses-to-$BRANCH-$NONCE" -m "AUTO: Add output files."
git push origin "$BRANCH:responses-to-$BRANCH-latest" \
  "responses-to-$BRANCH-$NONCE"

# Remove temporary directories used in the process.
cd /
rm -rf $TEMP_DIRECTORY