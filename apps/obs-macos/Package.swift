// swift-tools-version: 5.10
import PackageDescription

let package = Package(
    name: "ObsMacOSAppScaffold",
    platforms: [.macOS(.v14)],
    products: [
        .library(name: "ObsMacOSAppScaffold", targets: ["ObsMacOSAppScaffold"]),
        .executable(name: "ObsMacOSApp", targets: ["ObsMacOSApp"])
    ],
    targets: [
        .target(name: "ObsMacOSAppScaffold"),
        .executableTarget(
            name: "ObsMacOSApp",
            dependencies: ["ObsMacOSAppScaffold"]
        ),
        .testTarget(name: "ObsMacOSAppScaffoldTests", dependencies: ["ObsMacOSAppScaffold"])
    ]
)
