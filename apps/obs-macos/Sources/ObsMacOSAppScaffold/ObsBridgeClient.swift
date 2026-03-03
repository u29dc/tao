import Foundation

public struct BridgeEnvelope<Value: Decodable>: Decodable {
    public let schemaVersion: String
    public let ok: Bool
    public let value: Value?
    public let error: BridgeErrorDTO?
}

public struct BridgeErrorDTO: Decodable, Error {
    public let code: String
    public let message: String
    public let hint: String?
    public let context: [String: String]
}

public struct BridgeVaultStats: Decodable {
    public let vaultRoot: String
    public let filesTotal: UInt64
    public let markdownFiles: UInt64
    public let dbHealthy: Bool
    public let lastIndexUpdatedAt: String?
}

public struct BridgeNoteView: Decodable {
    public let path: String
    public let title: String
    public let frontMatter: String?
    public let body: String
    public let headingsTotal: UInt64
}

public enum ObsBridgeClientError: Error, CustomStringConvertible {
    case launchFailed(String)
    case processFailed(Int32, String)
    case decodeFailed(String)
    case bridgeError(BridgeErrorDTO)
    case missingValue

    public var description: String {
        switch self {
        case .launchFailed(let message):
            return "launch failed: \(message)"
        case .processFailed(let code, let stderr):
            return "process failed (\(code)): \(stderr)"
        case .decodeFailed(let message):
            return "decode failed: \(message)"
        case .bridgeError(let error):
            return "bridge error \(error.code): \(error.message)"
        case .missingValue:
            return "bridge envelope missing value payload"
        }
    }
}

public struct ObsBridgeClient {
    private let bridgeCommand: [String]
    private let repositoryRoot: URL

    public init(
        bridgeCommand: [String] = ["cargo", "run", "--quiet", "-p", "obs-sdk-bridge", "--"],
        repositoryRoot: URL? = nil
    ) {
        self.bridgeCommand = bridgeCommand
        self.repositoryRoot = repositoryRoot ?? Self.defaultRepositoryRoot()
    }

    public func vaultStats(vaultRoot: String, dbPath: String) throws -> BridgeVaultStats {
        try invoke(
            subcommand: [
                "vault-stats",
                "--vault-root", vaultRoot,
                "--db-path", dbPath
            ],
            as: BridgeVaultStats.self
        )
    }

    public func noteGet(vaultRoot: String, dbPath: String, path: String) throws -> BridgeNoteView {
        try invoke(
            subcommand: [
                "note-get",
                "--vault-root", vaultRoot,
                "--db-path", dbPath,
                "--path", path
            ],
            as: BridgeNoteView.self
        )
    }

    private func invoke<Value: Decodable>(subcommand: [String], as type: Value.Type) throws -> Value {
        let payload = try runProcess(arguments: bridgeCommand + subcommand)
        let decoder = JSONDecoder()
        decoder.keyDecodingStrategy = .convertFromSnakeCase

        let envelope: BridgeEnvelope<Value>
        do {
            envelope = try decoder.decode(BridgeEnvelope<Value>.self, from: payload)
        } catch {
            throw ObsBridgeClientError.decodeFailed(error.localizedDescription)
        }

        if envelope.ok {
            guard let value = envelope.value else {
                throw ObsBridgeClientError.missingValue
            }
            return value
        }

        if let error = envelope.error {
            throw ObsBridgeClientError.bridgeError(error)
        }
        throw ObsBridgeClientError.missingValue
    }

    private func runProcess(arguments: [String]) throws -> Data {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        process.arguments = arguments
        process.currentDirectoryURL = repositoryRoot

        let stdoutPipe = Pipe()
        let stderrPipe = Pipe()
        process.standardOutput = stdoutPipe
        process.standardError = stderrPipe

        do {
            try process.run()
        } catch {
            throw ObsBridgeClientError.launchFailed(error.localizedDescription)
        }

        process.waitUntilExit()
        let stdout = stdoutPipe.fileHandleForReading.readDataToEndOfFile()
        let stderrData = stderrPipe.fileHandleForReading.readDataToEndOfFile()
        let stderr = String(data: stderrData, encoding: .utf8) ?? ""

        guard process.terminationStatus == 0 else {
            throw ObsBridgeClientError.processFailed(process.terminationStatus, stderr)
        }
        return stdout
    }

    private static func defaultRepositoryRoot() -> URL {
        var root = URL(fileURLWithPath: #filePath)
        for _ in 0..<5 {
            root.deleteLastPathComponent()
        }
        return root
    }
}
