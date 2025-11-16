import Foundation
import SwiftLMDB

@available(macOS 10.15, *)
actor Reducer {
    typealias MapStream = AsyncValueStore<(String, Int)>

    private var stream: MapStream
    private let db: Database
    private var inMemoryCache = [String: Int]()
    private let flushThreshold: Int
    private let reducerNumber: Int
    private let fileManager: FileManager

    private var itemsProcessed = 0

    init(
        reducerNumber: Int,
        stream: MapStream,
        flushThreshold: Int = 100_000,
        fileManager: FileManager = .default
    ) throws {
        let dbGenerator = DbGenerator(fileManager: fileManager)
        self.db = try dbGenerator.createDb(reducerNumber: reducerNumber)
        // Ensure database is clear for the operation
        try db.empty()

        self.flushThreshold = flushThreshold
        self.stream = stream
        self.reducerNumber = reducerNumber
        self.fileManager = fileManager
    }

    func reduce() async throws {
        for await (key, value) in await stream.stream {
            try await self.processKeyValue(key: key, value: value)
        }

        // Final flush of remaining items
        try flushCacheToDb()

        try buildOutputFile()

        // The db is already cleared on creation and refresh
        // This is an attempt to do a final clear to not leave dangling temp files
        // That's why we ignore failures, since the output is already generated
        try? db.empty()
    }

    func refresh(streams: MapStream) throws {
        self.stream = streams
        try db.empty()
    }

    private func processKeyValue(key: String, value: Int) async throws {
        inMemoryCache[key, default: 0] += value
        itemsProcessed += 1

        if itemsProcessed >= flushThreshold {
            try flushCacheToDb()
            itemsProcessed = 0
        }
    }

    private func flushCacheToDb() throws {
        for (key, value) in inMemoryCache {
            if try db.exists(key: key) {
                if let cur = try db.get(type: Int.self, forKey: key) {
                    try db.put(value: value + cur, forKey: key)
                }
            } else {
                try db.put(value: value, forKey: key)
            }
        }
        inMemoryCache.removeAll(keepingCapacity: true)
    }

    private func buildOutputFile() throws {
        let fileName = "\(reducerNumber).r"
        let dirURL = URL(fileURLWithPath: fileManager.currentDirectoryPath)
            .appendingPathComponent("Output")

        try fileManager.createDirectory(at: dirURL, withIntermediateDirectories: true)

        let fileURL = dirURL.appendingPathComponent(fileName)
        if !fileManager.createFile(atPath: fileURL.path, contents: nil, attributes: nil) {
            throw "Error creating output file #\(reducerNumber)"
        }

        let handler = try FileHandle(forWritingTo: fileURL)
        defer { try? handler.close() }

        for (keyData, valueData) in db {
            guard let key = String(data: keyData, encoding: .utf8) else {
                continue
            }

            let value = valueData.withUnsafeBytes { $0.load(as: Int.self) }

            let str = "\(key) \(value)\n"
            if let data = str.data(using: .utf8) {
                handler.write(data)
            }
        }
    }
}

private struct DbGenerator {
    private static let path = "ReducerDb"
    private let fileManager: FileManager

    init(fileManager: FileManager) {
        self.fileManager = fileManager
    }

    func createDb(reducerNumber: Int) throws -> Database {
        let environment: Environment
        let database: Database

        let tmpDir = fileManager.temporaryDirectory
        let envURL =
            tmpDir
            .appendingPathComponent(Self.path)
            .appendingPathComponent("\(reducerNumber)")

        // Will silently proceed is directory already exists
        try fileManager.createDirectory(
            at: envURL, withIntermediateDirectories: true, attributes: nil)

        environment = try Environment(path: envURL.path, flags: [], maxDBs: 32)
        database = try environment.openDatabase(named: "db\(reducerNumber)", flags: [.create])

        return database
    }
}
