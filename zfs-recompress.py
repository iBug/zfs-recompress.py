#!/usr/bin/python3

import argparse
import glob
import humanize
import os
import pathlib
import queue
import shutil
import threading
import time
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
    total, used, free = shutil.disk_usage(filename)
    return free


def format_size(size: int) -> str:
    return humanize.naturalsize(size, binary=True, format="%.2f")


def gen_uuid() -> str:
    return str(uuid.uuid4())


def force_rm(filename: str) -> None:
    try:
        os.remove(filename)
    except OSError:
        pass


def cp_preserved(src: str, dst: str) -> None:
    shutil.copy2(src, dst)
    st = os.stat(src)
    os.chown(dst, st.st_uid, st.st_gid)


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
        raise OSError("Not enough free space to process file: {}".format(filename))

    try:
        workfilename = filename + WORKING_SUFFIX
        st_1 = os.stat(filename)
        cp_preserved(filename, workfilename)
        st_2 = os.stat(filename)
        if st_1.st_ino != st_2.st_ino or st_1.st_mtime != st_2.st_mtime:
            raise OSError("File changed during copy: {}".format(filename))
        force_mv(workfilename, filename)
    finally:
        force_rm(workfilename)


def worker_thread(qin: queue.SimpleQueue, qout: queue.SimpleQueue):
    while True:
        filename = qin.get()
        if filename is None:
            return
        size = get_file_size(filename)
        if should_skip_file(filename):
            continue
        qout.put((filename, size))
        process_file(filename)


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
            display_name = truncate_filename(filename)
            clear_line()
            print(f"Processed {count}: {display_name} ({format_size(size)} / {format_size(total_size)})", end="", flush=True)
    clear_line()
    print(f"Processed {count} files, {format_size(total_size)} total")

def get_files(path):
    for p in pathlib.Path(path).glob("**/*"):
        yield str(p)


def main() -> None:
    cwd = os.getcwd()
    qin = queue.SimpleQueue()
    qout = queue.SimpleQueue()
    t = threading.Thread(target=display_thread, args=(qout,), daemon=True)
    t.start()
    for i in range(4):
        spawn_worker(qin, qout)
    for filename in get_files(cwd):
        qin.put(filename)
    while not qin.empty():
        time.sleep(1)
    qout.put((None, 0))
    t.join()


if __name__ == "__main__":
    main()
