import Foundation

@available(macOS 10.15, *)
actor AsyncValueStore<Value: Sendable> {
    private var continuation: AsyncStream<Value>.Continuation?
    private(set) var stream: AsyncStream<Value>

    init() {
        var cont: AsyncStream<Value>.Continuation!
        self.stream = AsyncStream { continuation in
            cont = continuation
        }
        self.continuation = cont
    }

    func send(_ value: Value) async {
        continuation?.yield(value)
    }

    func finish() {
        continuation?.finish()
    }
}
