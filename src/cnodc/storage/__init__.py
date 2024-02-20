"""
    Provides general storage functionality.

    In general, one should use the StorageController to get a handle to a file.
    This handle knows how to perform various operations on the file as appropriate,
    regardless of if it is in the cloud or on a local drive.

    It is worth noting that, given the differences between file systems and the inherent
    differences between a remote cloud system, a local disk, and a network drive, not
    every operation is supported (e.g. file metadata and storage tiering) and the manner
    in which some operations (such as search()) are implemented are based on my own decisions
    at the time. This module operates on a best effort principle to provide a similar
    functionality whenever possible across different storage solutions.

    One key note: for URL-based storage, there can be an ambiguity when it comes to
    a directory name vs. a file name - for example, is

    ftp://example.com/hello-world

    a directory or a file? Without a server call, it can be difficult to determine and
    in some storage system, the distinction is not made (i.e. it can be both!).

    With that in mind, this component adopts a strict convention that URL-based
    directories end with a trailing back-slash (e.g. ftp://example.com/directory/)
    and files without one (e.g. ftp://example.com/file).

    To accommodate this, users must use the `child()` method when they wish to obtain
    a file that is the child of the current element (i.e. without a trailing slash)
    and `subdir()` when they wish to obtain a subdirectory. URL-based file handles
    will add slashes appropriately and use them to determine if the handle is a
    directory or not.

    Local file system paths do not need to follow this convention and will make a system
    call to determine if something is a directory or a file.
"""
from .core import StorageController
from .base import BaseStorageHandle
