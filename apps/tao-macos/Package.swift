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
        .target(name: "TaoMacOSAppScaffold"),
        .executableTarget(
            name: "TaoMacOSApp",
            dependencies: ["TaoMacOSAppScaffold"]
        ),
        .testTarget(name: "TaoMacOSAppScaffoldTests", dependencies: ["TaoMacOSAppScaffold"])
    ]
)
