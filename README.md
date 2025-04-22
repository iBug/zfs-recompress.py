# zfs-recompress.py

Rewrite of [gary17/zfs-recompress](https://github.com/gary17/zfs-recompress) in Python for better performance.

## Features

- Supports both Linux and BSD systems (with Python)
- Better performance without relying on system utilities
- Multi-threaded I/O operation (default: 8 threads)

## Usage

Run `zfs-recompress.py` in the directory that you want to process. Subdirectories are automatically handled.

Alternatively, use `zfs-recompress.py --help` to view available command-line options.

## Acknowledgements

- @gary17 for the idea and the original shell script
- @HPPinata and @cheriimoya for various improvements
