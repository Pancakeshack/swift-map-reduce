import Foundation

struct Splitter {
    let fileManager: FileManager = .default
    let bufferSize: Int = 1024 * 1024  // 1 MB

    @available(macOS 12.0, *)
    func split(filePath: String, numberOfSplits: Int = 4) async throws -> [URL] {
        let fileUrl = URL(fileURLWithPath: filePath)

        let attributes = try fileManager.attributesOfItem(atPath: fileUrl.path)
        guard let fileSize = attributes[.size] as? UInt64 else {
            throw "Unable to determine file size."
        }

        // Remainder will be included in final chunk
        let chunkSize = fileSize / UInt64(numberOfSplits)
        let tmpDirectory = fileManager.temporaryDirectory

        let reader = try FileHandle(forReadingFrom: fileUrl)
        defer { try? reader.close() }

        var createdFiles: [URL] = []
        var writers: [FileHandle] = []
        for i in 0..<numberOfSplits {
            let chunkFileUrl = tmpDirectory.appendingPathComponent("chunk.part\(i + 1)")
            fileManager.createFile(atPath: chunkFileUrl.path, contents: nil)
            let chunkFileHandle = try FileHandle(forWritingTo: chunkFileUrl)
            createdFiles.append(chunkFileUrl)
            writers.append(chunkFileHandle)
        }

        var currentPart = 1
        var bytesInCurrentPart: UInt64 = 0
        var curHandler = writers[currentPart - 1]
        var buffer = Data(capacity: bufferSize)

        for try await chunk in reader.bytes.lines {
            let chunkLine = chunk + "\n"
            guard let line = chunkLine.data(using: .utf8) else {
                throw "Error converting line to chunk"
            }
            buffer.append(line)
            if buffer.count == bufferSize || UInt64(buffer.count) + bytesInCurrentPart >= chunkSize
            {
                curHandler.write(buffer)
                bytesInCurrentPart += UInt64(buffer.count)
                buffer.removeAll()
            }
            if bytesInCurrentPart >= chunkSize && currentPart < numberOfSplits {
                curHandler = writers[currentPart]
                currentPart += 1
                bytesInCurrentPart = 0
            }
        }

        curHandler.write(buffer)
        for writer in writers {
            try? writer.close()
        }

        return createdFiles
    }

    func deleteChunkFiles(_ files: [URL]) {
        for fileUrl in files {
            do {
                try fileManager.removeItem(at: fileUrl)
            } catch {
                print("Failed to delete file at \(fileUrl): \(error)")
            }
        }
    }
}
