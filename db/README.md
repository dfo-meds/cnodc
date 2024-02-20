This directory contains the SQL file that creates an empty database from scratch (v001_000.sql). Once 
the design is finalized and being used, future updates should be stored in new files using major/minor
semantic versioning. 

The Dockerfile and compose file can be used to spin up a clean local copy of the database running on 
port 5432 (username example, password example) and an Adminer web client that you can use to view and
manipulate it running on port 8080.
