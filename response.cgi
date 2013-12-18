#!/bin/bash
#
# This script adds the specified main.ipynb file to the given repository.

set -e -x

# Prepare scripts.
TEMP_DIRECTORY="$(mktemp -d)"
NONCE="$(openssl sha1 count=1024 bs=1024 < /dev/random 2>/dev/null)"

cd $TEMP_DIRECTORY

# Modify repository.
git clone /home/git/repositories/assignment-one.git -b "$QUERY_STRING" . \
  1</dev/null
cat > ./main.ipynb

git add ./main.ipynb
git commit --allow-empty --author="Cylon Jeremy <open-source@fatlotus.com>" \
  -m "AUTO: Add output of main.ipynb to repository."

git tag -a "responses-to-$QUERY_STRING-$NONCE" -m \
  "AUTO: Add execution output to repository."
git push origin "$QUERY_STRING:responses-to-$QUERY_STRING-latest" \
  "responses-to-$QUERY_STRING-$NONCE"

# Cleanup
cd /
rm -rf $TEMP_DIRECTORY