# Quality Control Checks

QC checks typically fail with an error code and a flag. 
The flag is set as a `WorkingQuality` metadata element which is later turned into a `Quality` element when QC is finished.
The error code is used to provide useful information to anyone performing a manual review or who wants to understand why a test failed.

Tests are organized into different test suites, see below for details.

Most tests are performed on an individual OCPROC2 record. 
Some may be performed on a batch of records instead.

## Flag Values

These are an extension of the GTSPP QC flags. 
Flags 0-9 can be interpreted as per GTSPP. 
A value of 5 is treated exactly the same as a value of 1 except that it was manually set by the user.
Flags 11-19 are used to indicate that the QC algorithm recommends an action to the manual reviewer.
15 
Flags 20+ are used for errors that don't result from the QC process itself (e.g. errors in the data structure).

| Flag Number | Meaning                                                       | 
|-------------|---------------------------------------------------------------|
| 0           | Not tested                                                    |
| 1           | Good (as per GTSPP)                                           |
| 2           | Probably Good (as per GTSPP)                                  |
| 3           | Dubious (as per GTSPP)                                        |
| 4           | Bad (as per GTSPP)                                            | 
| 5           | Manually adjusted and confirmed to be good                    | 
| 9           | Missing or empty                                              |
| 13          | Recommended by QC algorithm to be set to 3                    |
| 14          | Recommended by QC algorithm to be set to 4                    | 
| 15          | Recommended change by the QC algorithm.                       | 
| 20          | Errors in the construction of the OCPROC2 record itself       |
| 21          | Errors in the QC process unrelated to the record being tested | 


## NODB Integrity Check
These checks verify that the OCPROC2 record conforms to the OCPROC2 ontology

| Error Code                              | Flag | Occurs On  | Meaning                                                                                                                                              |
|-----------------------------------------|------|------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| integrity_invalid_units                 | 20   | Units      | The units provided are not recognized by the UDUnits library.                                                                                        |
| integrity_missing_subrecord_coordinates | N/A  | Subrecords | A subrecord does not have at least one of the necessary coordinates for its given type (e.g. a PROFILE subrecord doesn't provide DEPTH or PRESSURE). |
| integrity_invalid_recordset_type        | N/A  | Recordsets | The type of subrecords is not recognized                                                                                                             | 
| integrity_undefined_element             | 20   | Elements   | The element name is not defined                                                                                                                      |
| integrity_invalid_group                 | 20   | Elements   | The element is in an inappropriate place in the data structure (e.g. a parameter is in the coordinates)                                              |
| integrity_no_allowed_groups             | 21   | Elements   | The element doesn't have a defined group in the vocabulary.                                                                                          | 
| integrity_multi_not_allowed             | 20   | Elements   | The element is multi-valued, but the vocabulary does not allow this.                                                                                 |
| integrity_incompatible_units            | 20   | Elements   | The vocabulary defines a preferred unit, but the element is in incompatible units or does not provide units.                                         |
| integrity_invalid_datetime              | 20   | Elements   | The vocabulary requires this to be an ISO-formatted date or datetime string and the element cannot be interpreted as such a string.                  |
| integrity_invalid_integer               | 20   | Elements   | The vocabulary requires this to be an integer or compatible string and the element is not one of these types.                                        |
| integrity_invalid_decimal               | 20   | Elements   | The vocabulary requires this to be an integer, decimal, or compatible string and the element is not one of these types.                              |
| integrity_invalid_string                | 20   | Elements   | The vocabulary requires this to be an integer, decimal, or string and the element is not one of these types.                                         | 
| integrity_invalid_list                  | 20   | Elements   | The vocabulary requires this to be a list and the element is not one of these types.                                                                 |
| integrity_invalid_element               | 20   | Elements   | The vocabulary requires this to be an OCPROC2 element and the element is not one of these types.                                                     |
| integrity_lower_than_range              | 14   | Elements   | The vocabulary defines a minimum value and the element value is strictly less than it.                                                               |
| integrity_greater_than_range            | 14   | Elements   | The vocabulary defines a maximum value and the element value is strictly greater than it.                                                            |
| integrity_value_not_allowed             | 14   | Elements   | The vocabulary defines a list of allowed values and the element value is not in that list                                                            |
| integrity_data_type_not_allowed         | 20   | Elements   | The vocabulary defines allowed values but the data type is not string or integer.                                                                    |  
