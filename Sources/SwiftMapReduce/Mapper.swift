import Foundation

@available(macOS 12.0, *)
actor Mapper {
    typealias Reducer = AsyncValueStore<(String, Int)>
    var reducers: [Reducer]
    let chunkFile: URL
    let fileManager: FileManager

    private var reducersAlive: Bool {
        reducers.count > 0
    }

    private var reducerCount: Int {
        reducers.count
    }

    init(
        reducers: [Reducer],
        chunkFile: URL,
        fileManager: FileManager = .default
    ) throws {
        if reducers.isEmpty {
            throw "Reducers array provided to Mapper is empty"
        }
        self.reducers = reducers
        self.chunkFile = chunkFile
        self.fileManager = fileManager
    }

    func map() async throws {
        if !reducersAlive {
            throw "Mapper has been called after reducers terminated!"
        }

        let handler = try FileHandle(forReadingFrom: chunkFile)
        defer { try? handler.close() }

        for try await line in handler.bytes.lines {
            for word in line.split(whereSeparator: \.isWhitespace) {
                let str = String(word)
                let reducer = reducerForKey(key: str)
                await reducer.send((str, 1))
            }
        }

        await finishReducers()
    }

    func refreshReducers(reducers: [Reducer]) {
        self.reducers = reducers
    }

    private func reducerForKey(key: String) -> Reducer {
        let index = abs(key.hash) % reducerCount
        return reducers[index]
    }

    private func finishReducers() async {
        for reducer in reducers {
            await reducer.finish()
        }
        reducers = []
    }
}
