#!/usr/bin/python3

import argparse
import os
import pathlib
import queue
import shutil
import threading
import uuid


WORKING_SUFFIX = ".zfs-recompress"


def clear_line():
    print(end="\x1B[2K\r")


def should_skip_file(filename: str) -> bool:
    if filename.endswith(WORKING_SUFFIX):
        return True
    return get_file_size(filename) <= 0


def get_file_size(filename: str) -> int:
    if os.path.islink(filename):
        # We don't process symlinks
        return 0
    if not os.path.isfile(filename):
        return 0
    return os.path.getsize(filename)


def get_free_space(filename: str) -> int:
    _, _, free = shutil.disk_usage(filename)
    return free


def format_size(size: float) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    for unit in units:
        if size >= 1024:
            size /= 1024
        else:
            return f"{size:.2f} {unit}"
    return f"{size:.2f} PiB"


def gen_uuid() -> str:
    return str(uuid.uuid4())


def force_rm(filename: str) -> None:
    try:
        os.remove(filename)
    except OSError:
        pass


def cp_preserved(src: str, dst: str) -> None:
    shutil.copy2(src, dst)
    stat = os.stat(src)
    os.chown(dst, stat.st_uid, stat.st_gid)


def force_mv(src: str, dst: str) -> None:
    try:
        os.rename(src, dst)
    except OSError:
        pass


def truncate_filename(filename: str, width: int = 24) -> str:
    filename = os.path.basename(filename)
    if len(filename) > width:
        filename = filename[:width - 8] + "..." + filename[-5:]
    return filename


def process_file(filename: str) -> None:
    if should_skip_file(filename):
        return
    size = get_file_size(filename)
    free = get_free_space(filename)
    if size > free:
        raise OSError(f"Not enough free space to process file: {filename}")

    workfilename = filename + WORKING_SUFFIX
    try:
        st_1 = os.stat(filename)
        cp_preserved(filename, workfilename)
        st_2 = os.stat(filename)
        if st_1.st_ino != st_2.st_ino or st_1.st_mtime != st_2.st_mtime:
            raise OSError(f"File changed during copy: {filename}")
        force_mv(workfilename, filename)
    finally:
        force_rm(workfilename)


def worker_thread(qin: queue.SimpleQueue, qout: queue.SimpleQueue):
    while True:
        filename = qin.get()
        if not filename:
            return
        size = get_file_size(filename)
        qout.put((filename, size))
        try:
            process_file(filename)
        except Exception as e:
            import traceback
            traceback.print_exc()


def spawn_worker(qin: queue.SimpleQueue, qout: queue.SimpleQueue) -> threading.Thread:
    t = threading.Thread(target=worker_thread, args=(qin, qout), daemon=True)
    t.start()
    return t


def display_thread(qin: queue.SimpleQueue):
    count, total_size = 0, 0
    while True:
        filename, size = qin.get()
        if filename is None:
            break
        count += 1
        total_size += size
        if count % 10 == 0 or size >= 20 << 20:
            # print every 10th file and anything larger than 20 MiB
            display_name = truncate_filename(filename, 24)
            clear_line()
            print(
                f"Processed {count}: {display_name:<24} ({format_size(size):>10} / {format_size(total_size):>10})",
                end="",
                flush=True,
            )
    clear_line()
    print(f"Processed {count} files, {format_size(total_size)} total")


def get_files(path: str):
    for p in pathlib.Path(path).glob("**/*"):
        yield str(p)


def parse_args():
    # cli arguments
    parser = argparse.ArgumentParser(
        description="Rewrite of [gary17/zfs-recompress](https://github.com/gary17/zfs-recompress) in Python for better performance"
    )
    parser.add_argument(
        "-f",
        "--folder",
        default="",
        help="process the specified FOLDER instead of the current working directory",
    )
    parser.add_argument(
        "-t",
        "--threads",
        type=int,
        default=8,
        help="number of threads to use. Default is 8 if unspecified",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    num_threads = args.threads
    cwd = os.getcwd() if args.folder == "" else args.folder
    qin = queue.SimpleQueue()
    qout = queue.SimpleQueue()
    t = threading.Thread(target=display_thread, args=(qout,), daemon=True)
    t.start()
    workers = []
    for i in range(num_threads):
        workers.append(spawn_worker(qin, qout))
    for filename in get_files(cwd):
        if should_skip_file(filename):
            continue
        qin.put(filename)
    for i in range(num_threads):
        qin.put(None)
    for w in workers:
        w.join()
    qout.put((None, 0))
    t.join()


if __name__ == "__main__":
    main()
