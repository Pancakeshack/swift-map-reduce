import Foundation

@available(macOS 12.0, *)
@main
enum SwiftMapReduce {
    static let mappersCount = 20
    static let reducersCount = 10
    static let input = "input.txt"

    static func main() async {
        let startTime: Date = Date()

        let streams: [AsyncValueStore<(String, Int)>] = (0..<Self.reducersCount).map { _ in
            AsyncValueStore<(String, Int)>()
        }

        do {
            print("Splitting file")
            let splitter = Splitter()
            let chunkFiles = try await splitter.split(
                filePath: Self.input, numberOfSplits: self.mappersCount)

            print("Starting map reduce")
            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    try await withThrowingTaskGroup(of: Void.self) { mapGroup in
                        for chunk in chunkFiles {
                            mapGroup.addTask {
                                let mapper = try Mapper(
                                    reducers: streams, chunkFile: chunk, )
                                try await mapper.map()
                            }
                        }

                        try await mapGroup.waitForAll()

                        // Mapping is done, finish continuations
                        for store in streams {
                            await store.finish()
                        }
                    }
                }

                for (index, value) in streams.enumerated() {
                    group.addTask {
                        let reducer = try Reducer(reducerNumber: index + 1, stream: value)
                        try await reducer.reduce()
                    }
                }

                try await group.waitForAll()
            }

            let elapsedTime = Date().timeIntervalSince(startTime)
            print("Completed in \(elapsedTime) seconds")
            print("Deleting temporary files")
            splitter.deleteChunkFiles(chunkFiles)
            print("Done!")
        } catch {
            print("There was an error during execution: \(error)")
        }
    }
}
