# processing-streamed-datarecords-from-kinesis-and-format-then-into-file-and-uploading-it-to-s3-bucket
This lambda code directly takes the datarecord from kinesis stream and then validates the set of metadata values atached in json body of datarecord and then it creates a file out the data tag in JSON body and extracts the metadata values in JSONbody and later attches those values to the created file and later it uploads the file to s3 bucket.
It also validates if those metadata values have been correctly sent if not the file created out of datarecord is sent to backup bucket.
