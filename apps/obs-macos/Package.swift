// swift-tools-version: 5.10
import PackageDescription

let package = Package(
    name: "ObsMacOSAppScaffold",
    platforms: [.macOS(.v14)],
    products: [
        .library(name: "ObsMacOSAppScaffold", targets: ["ObsMacOSAppScaffold"])
    ],
    targets: [
        .target(name: "ObsMacOSAppScaffold"),
        .testTarget(name: "ObsMacOSAppScaffoldTests", dependencies: ["ObsMacOSAppScaffold"])
    ]
)
