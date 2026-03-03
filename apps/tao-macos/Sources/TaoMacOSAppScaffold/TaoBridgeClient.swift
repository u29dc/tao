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

public struct BridgeNoteContext: Decodable {
    public let note: BridgeNoteView
    public let links: BridgeLinkPanels
}

public struct BridgeNoteSummary: Decodable {
    public let fileId: String
    public let path: String
    public let title: String
    public let updatedAt: String?
}

public struct BridgeNoteListPage: Decodable {
    public let items: [BridgeNoteSummary]
    public let nextCursor: String?
}

public struct BridgeWriteAck: Decodable {
    public let path: String
    public let fileId: String
    public let action: String
}

public struct BridgeEvent: Decodable {
    public let id: UInt64
    public let kind: String
    public let fileId: String?
    public let path: String?
    public let action: String?
    public let createdAt: String
}

public struct BridgeEventBatch: Decodable {
    public let events: [BridgeEvent]
    public let nextCursor: UInt64
}

public struct BridgeLinkRef: Decodable {
    public let sourcePath: String
    public let targetPath: String?
    public let heading: String?
    public let blockId: String?
    public let displayText: String?
    public let kind: String
    public let resolved: Bool
}

public struct BridgeLinkPanels: Decodable {
    public let outgoing: [BridgeLinkRef]
    public let backlinks: [BridgeLinkRef]
}

public struct BridgeBaseRef: Decodable {
    public let baseId: String
    public let filePath: String
    public let views: [String]
    public let updatedAt: String
}

public struct BridgeBaseColumn: Decodable {
    public let key: String
    public let label: String?
    public let hidden: Bool
    public let width: UInt16?
}

public struct BridgeBaseTableRow: Decodable, Identifiable {
    public let fileId: String
    public let filePath: String
    public let values: [String: String]

    public var id: String { fileId }
}

public struct BridgeBaseTablePage: Decodable {
    public let baseId: String
    public let filePath: String
    public let viewName: String
    public let page: UInt32
    public let pageSize: UInt32
    public let total: UInt64
    public let hasMore: Bool
    public let columns: [BridgeBaseColumn]
    public let rows: [BridgeBaseTableRow]
}

public enum TaoBridgeTypedError: Error, Equatable, CustomStringConvertible {
    case initFailed(BridgeErrorDTO)
    case vaultStatsFailed(BridgeErrorDTO)
    case noteGetInvalidPath(BridgeErrorDTO)
    case noteGetReadFailed(BridgeErrorDTO)
    case noteGetParseFailed(BridgeErrorDTO)
    case notesListInvalidLimit(BridgeErrorDTO)
    case notesListQueryFailed(BridgeErrorDTO)
    case noteLinksInvalidPath(BridgeErrorDTO)
    case noteLinksLookupFailed(BridgeErrorDTO)
    case noteLinksNotFound(BridgeErrorDTO)
    case noteLinksQueryFailed(BridgeErrorDTO)
    case basesListQueryFailed(BridgeErrorDTO)
    case basesListConfigFailed(BridgeErrorDTO)
    case basesViewInvalidInput(BridgeErrorDTO)
    case basesViewLookupFailed(BridgeErrorDTO)
    case basesViewNotFound(BridgeErrorDTO)
    case basesViewConfigFailed(BridgeErrorDTO)
    case basesViewPlanFailed(BridgeErrorDTO)
    case basesViewExecuteFailed(BridgeErrorDTO)
    case notePutInvalidPath(BridgeErrorDTO)
    case notePutLookupFailed(BridgeErrorDTO)
    case notePutCreateFailed(BridgeErrorDTO)
    case notePutUpdateFailed(BridgeErrorDTO)
    case notePutEventLogFailed(BridgeErrorDTO)
    case eventsPollInvalidLimit(BridgeErrorDTO)
    case eventsPollFailed(BridgeErrorDTO)
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
        case "bridge.notes_list.invalid_limit":
            return .notesListInvalidLimit(error)
        case "bridge.notes_list.query_failed":
            return .notesListQueryFailed(error)
        case "bridge.note_links.invalid_path":
            return .noteLinksInvalidPath(error)
        case "bridge.note_links.lookup_failed":
            return .noteLinksLookupFailed(error)
        case "bridge.note_links.not_found":
            return .noteLinksNotFound(error)
        case "bridge.note_links.query_failed":
            return .noteLinksQueryFailed(error)
        case "bridge.bases_list.query_failed":
            return .basesListQueryFailed(error)
        case "bridge.bases_list.config_failed":
            return .basesListConfigFailed(error)
        case "bridge.bases_view.invalid_input":
            return .basesViewInvalidInput(error)
        case "bridge.bases_view.lookup_failed":
            return .basesViewLookupFailed(error)
        case "bridge.bases_view.not_found":
            return .basesViewNotFound(error)
        case "bridge.bases_view.config_failed":
            return .basesViewConfigFailed(error)
        case "bridge.bases_view.plan_failed":
            return .basesViewPlanFailed(error)
        case "bridge.bases_view.execute_failed":
            return .basesViewExecuteFailed(error)
        case "bridge.note_put.invalid_path":
            return .notePutInvalidPath(error)
        case "bridge.note_put.lookup_failed":
            return .notePutLookupFailed(error)
        case "bridge.note_put.create_failed":
            return .notePutCreateFailed(error)
        case "bridge.note_put.update_failed":
            return .notePutUpdateFailed(error)
        case "bridge.note_put.event_log_failed":
            return .notePutEventLogFailed(error)
        case "bridge.events_poll.invalid_limit":
            return .eventsPollInvalidLimit(error)
        case "bridge.events_poll.failed":
            return .eventsPollFailed(error)
        case "bridge.serialize.failed":
            return .serializeFailed(error)
        default:
            return .unknown(error)
        }
    }

    public var bridgeCode: String {
        errorPayload.code
    }

    public var bridgeHint: String? {
        errorPayload.hint
    }

    public var bridgeContext: [String: String] {
        errorPayload.context
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
        case .notesListInvalidLimit(let error):
            return "notes list invalid limit: \(error.message)"
        case .notesListQueryFailed(let error):
            return "notes list query failed: \(error.message)"
        case .noteLinksInvalidPath(let error):
            return "note links invalid path: \(error.message)"
        case .noteLinksLookupFailed(let error):
            return "note links lookup failed: \(error.message)"
        case .noteLinksNotFound(let error):
            return "note links not found: \(error.message)"
        case .noteLinksQueryFailed(let error):
            return "note links query failed: \(error.message)"
        case .basesListQueryFailed(let error):
            return "bases list query failed: \(error.message)"
        case .basesListConfigFailed(let error):
            return "bases list config failed: \(error.message)"
        case .basesViewInvalidInput(let error):
            return "bases view invalid input: \(error.message)"
        case .basesViewLookupFailed(let error):
            return "bases view lookup failed: \(error.message)"
        case .basesViewNotFound(let error):
            return "bases view not found: \(error.message)"
        case .basesViewConfigFailed(let error):
            return "bases view config failed: \(error.message)"
        case .basesViewPlanFailed(let error):
            return "bases view plan failed: \(error.message)"
        case .basesViewExecuteFailed(let error):
            return "bases view execute failed: \(error.message)"
        case .notePutInvalidPath(let error):
            return "note put invalid path: \(error.message)"
        case .notePutLookupFailed(let error):
            return "note put lookup failed: \(error.message)"
        case .notePutCreateFailed(let error):
            return "note put create failed: \(error.message)"
        case .notePutUpdateFailed(let error):
            return "note put update failed: \(error.message)"
        case .notePutEventLogFailed(let error):
            return "note put event log failed: \(error.message)"
        case .eventsPollInvalidLimit(let error):
            return "events poll invalid limit: \(error.message)"
        case .eventsPollFailed(let error):
            return "events poll failed: \(error.message)"
        case .serializeFailed(let error):
            return "bridge serialize failed: \(error.message)"
        case .unknown(let error):
            return "bridge unknown error \(error.code): \(error.message)"
        }
    }

    private var errorPayload: BridgeErrorDTO {
        switch self {
        case .initFailed(let error),
            .vaultStatsFailed(let error),
            .noteGetInvalidPath(let error),
            .noteGetReadFailed(let error),
            .noteGetParseFailed(let error),
            .notesListInvalidLimit(let error),
            .notesListQueryFailed(let error),
            .noteLinksInvalidPath(let error),
            .noteLinksLookupFailed(let error),
            .noteLinksNotFound(let error),
            .noteLinksQueryFailed(let error),
            .basesListQueryFailed(let error),
            .basesListConfigFailed(let error),
            .basesViewInvalidInput(let error),
            .basesViewLookupFailed(let error),
            .basesViewNotFound(let error),
            .basesViewConfigFailed(let error),
            .basesViewPlanFailed(let error),
            .basesViewExecuteFailed(let error),
            .notePutInvalidPath(let error),
            .notePutLookupFailed(let error),
            .notePutCreateFailed(let error),
            .notePutUpdateFailed(let error),
            .notePutEventLogFailed(let error),
            .eventsPollInvalidLimit(let error),
            .eventsPollFailed(let error),
            .serializeFailed(let error),
            .unknown(let error):
            return error
        }
    }
}

public enum TaoBridgeClientError: Error, CustomStringConvertible {
    case launchFailed(String)
    case processFailed(Int32, String)
    case decodeFailed(String)
    case incompatibleSchema(expectedMajor: Int, actual: String)
    case bridgeError(TaoBridgeTypedError)
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

public struct TaoBridgeClient {
    private static let supportedSchemaMajor = 1
    private let bridgeCommand: [String]
    private let repositoryRoot: URL

    public init(
        bridgeCommand: [String] = ["cargo", "run", "--quiet", "-p", "tao-sdk-bridge", "--"],
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

    public func noteContext(
        vaultRoot: String,
        dbPath: String,
        path: String
    ) throws -> BridgeNoteContext {
        try invoke(
            subcommand: [
                "note-context",
                "--vault-root", vaultRoot,
                "--db-path", dbPath,
                "--path", path
            ],
            as: BridgeNoteContext.self
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

    public func notesList(
        vaultRoot: String,
        dbPath: String,
        afterPath: String? = nil,
        limit: UInt64 = 128
    ) throws -> BridgeNoteListPage {
        var subcommand: [String] = [
            "notes-list",
            "--vault-root", vaultRoot,
            "--db-path", dbPath,
            "--limit", String(limit)
        ]
        if let afterPath {
            subcommand.append(contentsOf: ["--after-path", afterPath])
        }
        return try invoke(subcommand: subcommand, as: BridgeNoteListPage.self)
    }

    public func noteLinks(
        vaultRoot: String,
        dbPath: String,
        path: String
    ) throws -> BridgeLinkPanels {
        try invoke(
            subcommand: [
                "note-links",
                "--vault-root", vaultRoot,
                "--db-path", dbPath,
                "--path", path
            ],
            as: BridgeLinkPanels.self
        )
    }

    public func basesList(vaultRoot: String, dbPath: String) throws -> [BridgeBaseRef] {
        try invoke(
            subcommand: [
                "bases-list",
                "--vault-root", vaultRoot,
                "--db-path", dbPath
            ],
            as: [BridgeBaseRef].self
        )
    }

    public func basesView(
        vaultRoot: String,
        dbPath: String,
        pathOrId: String,
        viewName: String,
        page: UInt32 = 1,
        pageSize: UInt32 = 50
    ) throws -> BridgeBaseTablePage {
        try invoke(
            subcommand: [
                "bases-view",
                "--vault-root", vaultRoot,
                "--db-path", dbPath,
                "--path-or-id", pathOrId,
                "--view-name", viewName,
                "--page", String(page),
                "--page-size", String(pageSize)
            ],
            as: BridgeBaseTablePage.self
        )
    }

    public func eventsPoll(
        vaultRoot: String,
        dbPath: String,
        afterId: UInt64 = 0,
        limit: UInt64 = 128
    ) throws -> BridgeEventBatch {
        try invoke(
            subcommand: [
                "events-poll",
                "--vault-root", vaultRoot,
                "--db-path", dbPath,
                "--after-id", String(afterId),
                "--limit", String(limit)
            ],
            as: BridgeEventBatch.self
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
            throw TaoBridgeClientError.decodeFailed(error.localizedDescription)
        }

        guard Self.isCompatibleSchemaVersion(envelope.schemaVersion) else {
            throw TaoBridgeClientError.incompatibleSchema(
                expectedMajor: Self.supportedSchemaMajor,
                actual: envelope.schemaVersion
            )
        }

        if envelope.ok {
            guard let value = envelope.value else {
                throw TaoBridgeClientError.missingValue
            }
            return value
        }

        if let error = envelope.error {
            throw TaoBridgeClientError.bridgeError(.fromBridgeDTO(error))
        }
        throw TaoBridgeClientError.missingValue
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
            throw TaoBridgeClientError.launchFailed(error.localizedDescription)
        }

        process.waitUntilExit()
        let stdout = stdoutPipe.fileHandleForReading.readDataToEndOfFile()
        let stderrData = stderrPipe.fileHandleForReading.readDataToEndOfFile()
        let stderr = String(data: stderrData, encoding: .utf8) ?? ""

        guard process.terminationStatus == 0 else {
            throw TaoBridgeClientError.processFailed(process.terminationStatus, stderr)
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
