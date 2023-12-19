#!/bin/sh
new_version=$1

if [ -z "$new_version" ]; then
    echo "New version can't be empty" >&2
    exit 1
fi

find . -type f -name gradle.properties -exec sed -i "s|^version=\S\+|version=${new_version}|g" {} \;
