import Foundation

public struct BridgeEnvelope<Value: Decodable>: Decodable {
    public let schemaVersion: String
    public let ok: Bool
    public let value: Value?
    public let error: BridgeErrorDTO?
}

public struct BridgeErrorDTO: Decodable, Error, Equatable {
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

public struct BridgeWriteAck: Decodable {
    public let path: String
    public let fileId: String
    public let action: String
}

public enum ObsBridgeTypedError: Error, Equatable, CustomStringConvertible {
    case initFailed(BridgeErrorDTO)
    case vaultStatsFailed(BridgeErrorDTO)
    case noteGetInvalidPath(BridgeErrorDTO)
    case noteGetReadFailed(BridgeErrorDTO)
    case noteGetParseFailed(BridgeErrorDTO)
    case notePutInvalidPath(BridgeErrorDTO)
    case notePutLookupFailed(BridgeErrorDTO)
    case notePutCreateFailed(BridgeErrorDTO)
    case notePutUpdateFailed(BridgeErrorDTO)
    case serializeFailed(BridgeErrorDTO)
    case unknown(BridgeErrorDTO)

    static func fromBridgeDTO(_ error: BridgeErrorDTO) -> Self {
        switch error.code {
        case "bridge.init.failed":
            return .initFailed(error)
        case "bridge.vault_stats.failed":
            return .vaultStatsFailed(error)
        case "bridge.note_get.invalid_path":
            return .noteGetInvalidPath(error)
        case "bridge.note_get.read_failed":
            return .noteGetReadFailed(error)
        case "bridge.note_get.parse_failed":
            return .noteGetParseFailed(error)
        case "bridge.note_put.invalid_path":
            return .notePutInvalidPath(error)
        case "bridge.note_put.lookup_failed":
            return .notePutLookupFailed(error)
        case "bridge.note_put.create_failed":
            return .notePutCreateFailed(error)
        case "bridge.note_put.update_failed":
            return .notePutUpdateFailed(error)
        case "bridge.serialize.failed":
            return .serializeFailed(error)
        default:
            return .unknown(error)
        }
    }

    public var bridgeCode: String {
        switch self {
        case .initFailed(let error),
            .vaultStatsFailed(let error),
            .noteGetInvalidPath(let error),
            .noteGetReadFailed(let error),
            .noteGetParseFailed(let error),
            .notePutInvalidPath(let error),
            .notePutLookupFailed(let error),
            .notePutCreateFailed(let error),
            .notePutUpdateFailed(let error),
            .serializeFailed(let error),
            .unknown(let error):
            return error.code
        }
    }

    public var description: String {
        switch self {
        case .initFailed(let error):
            return "bridge init failed: \(error.message)"
        case .vaultStatsFailed(let error):
            return "vault stats failed: \(error.message)"
        case .noteGetInvalidPath(let error):
            return "note get invalid path: \(error.message)"
        case .noteGetReadFailed(let error):
            return "note get read failed: \(error.message)"
        case .noteGetParseFailed(let error):
            return "note get parse failed: \(error.message)"
        case .notePutInvalidPath(let error):
            return "note put invalid path: \(error.message)"
        case .notePutLookupFailed(let error):
            return "note put lookup failed: \(error.message)"
        case .notePutCreateFailed(let error):
            return "note put create failed: \(error.message)"
        case .notePutUpdateFailed(let error):
            return "note put update failed: \(error.message)"
        case .serializeFailed(let error):
            return "bridge serialize failed: \(error.message)"
        case .unknown(let error):
            return "bridge unknown error \(error.code): \(error.message)"
        }
    }
}

public enum ObsBridgeClientError: Error, CustomStringConvertible {
    case launchFailed(String)
    case processFailed(Int32, String)
    case decodeFailed(String)
    case incompatibleSchema(expectedMajor: Int, actual: String)
    case bridgeError(ObsBridgeTypedError)
    case missingValue

    public var description: String {
        switch self {
        case .launchFailed(let message):
            return "launch failed: \(message)"
        case .processFailed(let code, let stderr):
            return "process failed (\(code)): \(stderr)"
        case .decodeFailed(let message):
            return "decode failed: \(message)"
        case .incompatibleSchema(let expectedMajor, let actual):
            return "incompatible schema version \(actual), expected major v\(expectedMajor)"
        case .bridgeError(let error):
            return error.description
        case .missingValue:
            return "bridge envelope missing value payload"
        }
    }
}

public struct ObsBridgeClient {
    private static let supportedSchemaMajor = 1
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

    public func notePut(
        vaultRoot: String,
        dbPath: String,
        path: String,
        content: String
    ) throws -> BridgeWriteAck {
        try invoke(
            subcommand: [
                "note-put",
                "--vault-root", vaultRoot,
                "--db-path", dbPath,
                "--path", path,
                "--content", content
            ],
            as: BridgeWriteAck.self
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

        guard Self.isCompatibleSchemaVersion(envelope.schemaVersion) else {
            throw ObsBridgeClientError.incompatibleSchema(
                expectedMajor: Self.supportedSchemaMajor,
                actual: envelope.schemaVersion
            )
        }

        if envelope.ok {
            guard let value = envelope.value else {
                throw ObsBridgeClientError.missingValue
            }
            return value
        }

        if let error = envelope.error {
            throw ObsBridgeClientError.bridgeError(.fromBridgeDTO(error))
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

    static func isCompatibleSchemaVersion(_ schemaVersion: String) -> Bool {
        guard let parsed = parseSchemaVersion(schemaVersion) else {
            return false
        }
        return parsed.major == supportedSchemaMajor
    }

    private static func parseSchemaVersion(_ raw: String) -> (major: Int, minor: Int)? {
        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        guard trimmed.hasPrefix("v") else {
            return nil
        }

        let numeric = String(trimmed.dropFirst())
        let parts = numeric.split(separator: ".", omittingEmptySubsequences: false)
        guard parts.count == 1 || parts.count == 2 else {
            return nil
        }

        guard let major = Int(parts[0]) else {
            return nil
        }

        if parts.count == 1 {
            return (major: major, minor: 0)
        }

        guard let minor = Int(parts[1]) else {
            return nil
        }
        return (major: major, minor: minor)
    }

    private static func defaultRepositoryRoot() -> URL {
        var root = URL(fileURLWithPath: #filePath)
        for _ in 0..<5 {
            root.deleteLastPathComponent()
        }
        return root
    }
}
