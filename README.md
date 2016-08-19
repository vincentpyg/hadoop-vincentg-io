#vghadoop-io

Custom InputFormats for MapReduce. Also useful in Spark.

##MultipleRawFileInputFormat
Reads the contents of multiple files, each stored into a ByteArrayInputStream. 
Useful for reading PDFs, Images, XML files, etc.

##MultipleTextFileInputFormat
Reads the contents of multiple files, each stored as a String. 
Useful for operations (i.e. Sentiment Analysis, Event Relation Extraction, Document Classification, etc.) that need to read the entire contents of a file.

##VgZipInputFormat
Reads multiple files inside a ZIP archive.
