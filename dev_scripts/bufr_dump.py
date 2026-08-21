import sys
import pathlib
import traceback
import os
import typing as t
sys.path.append(str(pathlib.Path(__file__).absolute().parent.parent / "src"))
from medsutil.ocproc2.codecs import GtsCodec, OCProc2BinCodec

try:
    scan_dir = sys.argv[1]
except IndexError:
    print("You must provide a path to scan for files")
    raise

expected = 0
for file in os.scandir(scan_dir):
    if not (file.name.endswith(".results") or file.name.endswith(".bin")):
        expected += 1
print(f"Found {expected} files")


wmo = GtsCodec()
bin = OCProc2BinCodec()
success = 0
failure = 0
skipped = 0
completed = 0
for file in os.scandir(scan_dir):
    if any(file.name.endswith(x) for x in (".results", ".bin")):
        skipped += 1
        continue
    base = pathlib.Path(file.path)
    result_file = base.with_name(base.name + ".results")
    if result_file.exists():
        completed += 1
        continue
    try:
        print(f"{success + failure + completed} / {expected} [{success} + {failure} + {completed} || {skipped}]  {file.name} - decoding                  ", end="\r")
        messages = [x for x in wmo.load(base, fail_on_error=True)]
        print(f"{success + failure + completed} / {expected} [{success} + {failure} + {completed} || {skipped}]  {file.name} - saving messages     ", end="\r")
        bin.dump(base.with_name(base.name + ".bin"), messages, codec="JSON", compression="LZMA3")
        print(f"{success + failure + completed} / {expected} [{success} + {failure} + {completed} || {skipped}]  {file.name} - saving results         ", end="\r")
        with open(result_file, "w") as h:
            h.write("File,Index,PlatformID,PlatformName,WMOID,WIGOSID,GTS Header,ASCII Form,BUFR Code,Latitude,Longitude,Time\n")
            for idx, message in enumerate(messages):
                values: list[str] = [
                    file.name or "????",
                    str(idx)
                ]
                values.extend(
                    message.metadata.best(x, coerce=str, default="")
                    for x in ("PlatformID", "PlatformName", "WMOID", "WIGOSID", "GTSHeader", "WMOAsciiCodeForm")
                )
                if message.metadata.has_value("BUFRDescriptors"):
                    values.append(";".join(str(x) for x in message.metadata["BUFRDescriptors"].value))
                else:
                    values.append("n/a")

                values.extend(
                    message.coordinates.best(x, coerce=str, default="")
                    for x in ("Latitude", "Longitude", "Time")
                )
                h.write(",".join(values) + "\n")

        success += 1
        print(f"{success + failure + completed} / {expected} [{success} + {failure} + {completed} || {skipped}]  {file.name} - complete                ", end="\r")
    except Exception as ex:
        with open(result_file, "w") as h:
            h.write(traceback.format_exc())
        traceback.print_exc()
        exit(1)
        failure += 1
        print(f"{success + failure + completed} / {expected} [{success} + {failure} + {completed} || {skipped}]  {file.name} - error                 ", end="\r")
    except KeyboardInterrupt:
        if result_file.exists():
            result_file.unlink()
        raise



