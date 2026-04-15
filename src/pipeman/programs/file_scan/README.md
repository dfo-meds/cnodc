# File Downloader

This module provides useful tools for many programs that enables 
the processing of files from other sources (e.g. SFTP).

The module consists of two parts: 

1. A scheduled task to scan and queue file downloads
2. A queue worker to download and process the files

The queue worker will use a workflow to process the file - see the workflow
documentation on how to set the upload and archival folders as well as 
queuing and validation.

## Sample Configuration

### File Scanner

```yaml
task_scanner:
  class_name: cnodc.programs.file_scan.FileScanTask
  config:
    queue_name: ''                  # The name of the queue that should process these files
    workflow_name: ''               # The workflow name to use to process the files
    scan_target: ''                 # Directory to scan (must be compatible with the storage system)
    pattern: '*'                    # The pattern to apply (see Python fnmatch for details)
    recursive: false                # Set to true to scan recursively in all sub-directories
    remove_downloaded_files: false  # Set to true to remove files once they have been downloaded
    headers:                        # A dictionary of header names and values that will be passed to the workflow
    # scheduled task info
```

### File Downloader
```yaml
task_downloader:
  class_name: cnodc.programs.file_scan.FileScanWorker
  config:
    queue_name: ''  # Matches above
    # queue info
```
