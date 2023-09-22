// Running a simple processor to see what is in a stream.
// In this case it's a change stream source. But this works
// with Kafka, etc as well.

// a source stage
source = {$source:
   {
      "connectionName":"myAtlasSource", 
      "db": "test", 
      "coll": "myTest"
   }
}

// create a processor with only 1 stage, just the source
processor = [s]

// let's take a peek at that stream
sp.process(processor)
