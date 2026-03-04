// swift-tools-version: 5.10
import PackageDescription

let package = Package(
    name: "TaoMacOSAppScaffold",
    platforms: [.macOS(.v14)],
    products: [
        .library(name: "TaoMacOSAppScaffold", targets: ["TaoMacOSAppScaffold"]),
        .executable(name: "TaoMacOSApp", targets: ["TaoMacOSApp"])
    ],
    targets: [
        .systemLibrary(
            name: "tao_sdk_bridgeFFI",
            path: "Sources/TaoMacOSAppScaffold/Generated"
        ),
        .target(
            name: "TaoMacOSAppScaffold",
            dependencies: ["tao_sdk_bridgeFFI"],
            linkerSettings: [
                .unsafeFlags([
                    "-L", "../../target/release",
                    "-Xlinker", "-rpath",
                    "-Xlinker", "@executable_path/../Frameworks"
                ]),
                .linkedLibrary("tao_sdk_bridge")
            ]
        ),
        .executableTarget(
            name: "TaoMacOSApp",
            dependencies: ["TaoMacOSAppScaffold"]
        ),
        .testTarget(name: "TaoMacOSAppScaffoldTests", dependencies: ["TaoMacOSAppScaffold"])
    ]
)
