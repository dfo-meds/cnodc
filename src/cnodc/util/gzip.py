import gzip
import pathlib
from cnodc.util.halts import HaltInterrupt, HaltFlag

# NB:
# Using shutil.copyfileobj() is fairly fast but doesn't have a halt flag. Therefore, a very big file
# (e.g. tbs) may cause significant issues during halting. The below methods allow a halt_flag to be
# passed which will halt the copy process and remove the target file. The chunk size was based on testing:
# 2.5 MiB per read translates to about 0.5 seconds between reads. Thus, splitting the
# file into roughly this size of chunks should allow the script to break within 0.5 seconds still.
# Overall performance is similar to using shutil.copyfileobj().

def ungzip_with_halt(source_file: pathlib.Path, target_file: pathlib.Path, chunk_size=2621440, halt_flag: HaltFlag = None):
    """Ungzip a file into the target file."""
    try:
        with gzip.open(source_file, 'rb') as src:
            with open(target_file, 'wb') as dest:
                if halt_flag is None:
                    src_bytes = src.read(chunk_size)
                    while src_bytes != b'':
                        dest.write(src_bytes)
                        src_bytes = src.read(chunk_size)
                else:
                    halt_flag.check_continue(True)
                    src_bytes = src.read(chunk_size)
                    while src_bytes != b'':
                        halt_flag.check_continue(True)
                        dest.write(src_bytes)
                        halt_flag.check_continue(True)
                        src_bytes = src.read(chunk_size)
    except HaltInterrupt as ex:
        target_file.unlink(True)
        raise ex from ex


def gzip_with_halt(source_file: pathlib.Path, target_file: pathlib.Path, chunk_size=2621440, halt_flag: HaltFlag = None):
    """Gzip a file into the target file."""
    try:
        with open(source_file, 'rb') as src:
            with gzip.open(target_file, 'wb') as dest:
                if halt_flag is None:
                    src_bytes = src.read(chunk_size)
                    while src_bytes != b'':
                        dest.write(src_bytes)
                        src_bytes = src.read(chunk_size)
                else:
                    halt_flag.check_continue(True)
                    src_bytes = src.read(chunk_size)
                    while src_bytes != b'':
                        halt_flag.check_continue(True)
                        dest.write(src_bytes)
                        halt_flag.check_continue(True)
                        src_bytes = src.read(chunk_size)
    except HaltInterrupt as ex:
        target_file.unlink(True)
        raise ex from ex
