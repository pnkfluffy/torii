#!/bin/sh
cd "$(dirname "$0")/.." || exit 1
exec /usr/bin/python3 scripts/setup.py
