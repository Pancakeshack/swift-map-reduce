// swift-tools-version: 6.2
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "SwiftMapReduce",
    dependencies: [
        .package(url: "https://github.com/agisboye/SwiftLMDB.git", from: "2.0.0")
    ],
    targets: [
        .executableTarget(
            name: "SwiftMapReduce",
            dependencies: [
                "SwiftLMDB"
            ]
        )
    ]
)
