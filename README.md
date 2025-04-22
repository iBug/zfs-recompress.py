# zfs-recompress.py

Rewrite of [gary17/zfs-recompress](https://github.com/gary17/zfs-recompress) in Python for better performance.

## Features

- Supports both Linux and BSD systems (with Python)
- Better performance without relying on system utilities
- 4 concurrent threads for I/O operation

## Usage

Run `zfs-recompress.py` in the directory where you want to process. Subdirectories are automatically handled.

## Acknowledgements

- @gary17 for the idea and the original shell script
- @HPPinata and @cheriimoya for various improvements
