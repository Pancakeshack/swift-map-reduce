# Swift Map Reduce Example

This project is an example of map reduce, based on the 2003 Google white paper for the monthly Heatsync Labs White Paper Book Club

Paper: https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf
Heatsync Labs: https://www.heatsynclabs.org

It aims to implement the core logic discussed in the paper on a smaller scale, using the word count processing example.

## How to run

The project includes a sample 17.8 MB and 262049 line input file for processing.

1. Ensure you have the swift toolchain installed, which includes the `swift` command. Swift is available on all platforms such as Mac, Linux, Windows, FreeBSD, etc.
You can find some information on installing swift here if you don't already have it: https://www.swift.org/getting-started/
2. Open a terminal in the root of the project and run `swift run -c release`
3. Check the created Output folder

## Brief Description

To simulate the GFS file chunking and distributed mapping and reducing logic discussed in the paper, I decided to heavily utilize Swift concurrency to parallelize across multiple threads. 

I prioritized scalability to match as closely to the file as possible. I made these considerations during development:

- Files are streamed line by line and not kept in memory. A 1mb buffer is filled and flushed periodically the the chunk files during splits.
- The map logic streams from the file line by line, and passes the mapped values through an `AsyncStream`.
- The reducer keeps in an memory cache of the reduced values, and periodically flushes the values to an embedded LMDB database. I utilized this great wrapper library the builds and embeds the C LMDB database for portability and wraps the low level calls: https://github.com/agisboye/SwiftLMDB.
- The output files are created by streaming the key value pairs from the embeded LMDB.

The core logic is as follows:

### Chunking

The program takes in the input file into 6 chunks. This is to simulate the chunking behavior of GFS, while keeping it all on one machine. They are stored in temporary files and returned from the chunker call.

### Mapping

The program creates 6 total mappers (equal to the number of chunks) and 3 total reducers. Generally in most Map Reduce implementations you'll have more mappers than you do reducers, since the mapping logic is generally more time consuming as it has to read through the files and I/O is expensive. When the mappers are created they are passed in an array of `AsyncStream`, one for each reducer, and a `URL` for the chunk they are processing. They then stream the chunk line by line and split the words into key value pairs of `(Word, 1)`. A hashing function is used to determine the reducer to send to so ensure reducers process the same words across all mappers, in pseudocode `hash(key) % reducers.count`.

### Reducing

The 3 reducers are passed in a single `AsyncStream` each. They create their embedded db, cachce, and get to work processing values sent from the mappers. They fill up a cache until a threshold of values have been processed and then flush it to the embedded db. Once the stream is cancelled the reducer assumes the map logic is done and they being processing the output files by reading from the db. A `reducer_number.r` text file is generated under the `Output` directory for each reducer, including their counts.