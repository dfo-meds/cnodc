# Castaway CTD
The castaway CTD program is designed to collect data from Castaway CTD devices, store it, and publish it on
an ERDDAP server.

## File Submissions
Files can be submitted into the program via scheduled task scraping or web API submissions. The queue items
should contain the following keys as a minimum:

- upload_file: the file that contains the CTD data in CTD CSV format
- gzip: a boolean set to True if the upload_file is gzipped (defaults to False)

## Processing
The program consists of a QueueWorker that is designed to perform the following steps:

1. Download the file to a local temporary file
2. Ungzip it, if necessary
3. Process it as a CTD CSV format and perform basic validation
4. Generate a NetCDF file according to the program format
5. Optionally, gzip the NetCDF file (the default, can be disabled for the ERDDAP but not the archival)
6. Upload the final NetCDF file to two locations: one for delivery via ERDDAP and one for archival. Note that raw and processed files may be uploaded to different locations.
7. Trigger a reload of the ERDDAP dataset, if configured

## Sample Configuration

### Processor
```yaml
castaway_ctd_processor:
    class_name: cnodc.programs.castaway_ctd.intake.CastawayIntakeWorker
    config:
        queue_name: ""                      # Name of the queue to process
        erddap_directory_raw: ""            # Path for ERDDAP raw castaway CTD files
        erddap_directory_processed: ""      # Path for ERDDAP processed castaway CTD files
        archive_directory_raw: ""           # Path for archival raw castaway CTD files
        archive_directory_processed: ""     # Path for archival processed castaway CTD files
        # delay_time_seconds: 0.25          # uncomment to change the initial time to delay if no queue item is found
        # delay_factor: 2                   # uncomment to change the factor to multiple the delay time by after each unsuccessful attempt to get a queue item
        # max_delay_time_seconds: 128       # uncomment to change the maximum amount of time to delay between checking the queue
        # retry_delay_seconds: 0            # uncomment to specify an amount of time to delay each failed queue item before retrying it
        # erddap_cluster: ""                # uncomment to specify a specific ERDDAP server (see application configuration) 
        # gzip: false                       # uncomment to avoid gzipping the output files for ERDDAP
        # erddap_dataset_id: ""             # Dataset ID to reload  
```

### Upload Workflow
```yaml
queue: ""                   # Queue name to enqueue the uploads in
upload: ""                  # Path to upload the files for processing
# upload_tier: "hot"        # Can set to frequent, infrequent or archival
upload_metadata:
  # AccessLevel: "GENERAL"
  # SecurityLabel: "UNCLASSIFIED"
  # PublicationPlan: "NONE"
  Program: "CASTAWAY_CTD"
  Dataset: "SUBMISSIONS"
  CostUnit: "MARITIMES"
# archive: ""               # Path to upload the files for archival, if desired
# archive_tier: "archive"   # Can set to frequent, infrequent or archival
archive_metadata:
  # AccessLevel: "GENERAL"
  # SecurityLabel: "UNCLASSIFIED"
  # PublicationPlan: "NONE"
  Program: "CASTAWAY_CTD"
  Dataset: "SUBMISSIONS"
  CostUnit: "MARITIMES"
# queue_priority: 0         # Set the priority in the queue, if desired
# allow_overwrite: 'user'   # Set to 'never', 'user' or 'always' depending on if you want people to be able to overwrite the source file
validation: "cnodc.programs.castaway_ctd.intake.validate_castaway_ctd_file
```


## Input Format

The program expects a CSV format which consists of the following:

1. A number of rows where the first cell starts with a percent symbol (`%`) followed by the name of a profile parameter. The second cell should contain the value or an empty string.
2. A row where the first cell is a percent symbol, followed by zero or more symbols to indicate the end of the profile parameter section.
3. A header row for the data section, where each cell contains the name of a level parameter
4. A number of rows of the same length of the header row, where each cell contains the value of the given parameter at a level
5. Optionally, one or more blank lines


# Output Format

The output format is a CF-compliant NetCDF file in DSG profile continuous ragged array format. The file will only 
contain one profile, but these are designed to work with ERDDAP to be assembled into a single larger file.
