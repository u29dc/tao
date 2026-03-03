import Testing
import Foundation
@testable import ObsMacOSAppScaffold

@Test func module_name_is_stable() {
    #expect(ObsMacOSAppScaffold.moduleName() == "ObsMacOSAppScaffold")
}

@Test func bridge_client_calls_vault_stats_and_note_get() throws {
    let fileManager = FileManager.default
    let tempRoot = fileManager.temporaryDirectory
        .appendingPathComponent("obs-bridge-test-\(UUID().uuidString)")
    defer { try? fileManager.removeItem(at: tempRoot) }

    let vaultRoot = tempRoot.appendingPathComponent("vault")
    let notesDir = vaultRoot.appendingPathComponent("notes")
    let dbPath = tempRoot.appendingPathComponent("obs.sqlite")
    try fileManager.createDirectory(at: notesDir, withIntermediateDirectories: true)
    try """
    ---
    status: draft
    ---
    # Alpha
    bridge test
    """.write(
        to: notesDir.appendingPathComponent("a.md"),
        atomically: true,
        encoding: .utf8
    )

    let client = ObsBridgeClient()
    let stats = try client.vaultStats(
        vaultRoot: vaultRoot.path,
        dbPath: dbPath.path
    )
    #expect(stats.dbHealthy)
    #expect(stats.filesTotal == 1)
    #expect(stats.markdownFiles == 1)

    let note = try client.noteGet(
        vaultRoot: vaultRoot.path,
        dbPath: dbPath.path,
        path: "notes/a.md"
    )
    #expect(note.path == "notes/a.md")
    #expect(note.title == "Alpha")
    #expect(note.headingsTotal == 1)
    #expect(note.body.contains("bridge test"))
}

@Test func bridge_client_note_put_creates_and_updates_notes() throws {
    let fileManager = FileManager.default
    let tempRoot = fileManager.temporaryDirectory
        .appendingPathComponent("obs-bridge-write-test-\(UUID().uuidString)")
    defer { try? fileManager.removeItem(at: tempRoot) }

    let vaultRoot = tempRoot.appendingPathComponent("vault")
    let notesDir = vaultRoot.appendingPathComponent("notes")
    let dbPath = tempRoot.appendingPathComponent("obs.sqlite")
    try fileManager.createDirectory(at: notesDir, withIntermediateDirectories: true)

    let client = ObsBridgeClient()
    let created = try client.notePut(
        vaultRoot: vaultRoot.path,
        dbPath: dbPath.path,
        path: "notes/from-swift.md",
        content: "# Swift Created\none"
    )
    #expect(created.path == "notes/from-swift.md")
    #expect(created.action == "created")

    let firstRead = try client.noteGet(
        vaultRoot: vaultRoot.path,
        dbPath: dbPath.path,
        path: "notes/from-swift.md"
    )
    #expect(firstRead.body.contains("one"))

    let updated = try client.notePut(
        vaultRoot: vaultRoot.path,
        dbPath: dbPath.path,
        path: "notes/from-swift.md",
        content: "# Swift Updated\ntwo"
    )
    #expect(updated.action == "updated")

    let secondRead = try client.noteGet(
        vaultRoot: vaultRoot.path,
        dbPath: dbPath.path,
        path: "notes/from-swift.md"
    )
    #expect(secondRead.title == "Swift Updated")
    #expect(secondRead.body.contains("two"))
}

@Test func bridge_client_accepts_compatible_minor_schema_versions() throws {
    #expect(ObsBridgeClient.isCompatibleSchemaVersion("v1"))
    #expect(ObsBridgeClient.isCompatibleSchemaVersion("v1.8"))
    #expect(!ObsBridgeClient.isCompatibleSchemaVersion("v2"))
    #expect(!ObsBridgeClient.isCompatibleSchemaVersion("v1.beta"))
}

@Test func bridge_client_rejects_incompatible_schema_from_bridge_output() throws {
    let fixture = try makeMockBridgeScript(
        payload: """
        {"schema_version":"v2.0","ok":true,"value":{"path":"notes/mock.md","title":"Mock","front_matter":null,"body":"mock body","headings_total":0},"error":null}
        """
    )
    defer { try? FileManager.default.removeItem(at: fixture.tempRoot) }

    let client = ObsBridgeClient(
        bridgeCommand: [fixture.script.path],
        repositoryRoot: fixture.tempRoot
    )

    do {
        _ = try client.noteGet(
            vaultRoot: fixture.tempRoot.path,
            dbPath: fixture.tempRoot.appendingPathComponent("obs.sqlite").path,
            path: "notes/mock.md"
        )
        Issue.record("expected schema compatibility failure")
    } catch let error as ObsBridgeClientError {
        switch error {
        case .incompatibleSchema(let expectedMajor, let actual):
            #expect(expectedMajor == 1)
            #expect(actual == "v2.0")
        default:
            Issue.record("unexpected bridge error: \(error)")
        }
    }
}

@Test func bridge_client_maps_known_bridge_error_codes_to_typed_errors() throws {
    let fixture = try makeMockBridgeScript(
        payload: """
        {"schema_version":"v1.0","ok":false,"value":null,"error":{"code":"bridge.note_put.update_failed","message":"update failed","hint":"retry","context":{"path":"notes/mock.md"}}}
        """
    )
    defer { try? FileManager.default.removeItem(at: fixture.tempRoot) }

    let client = ObsBridgeClient(
        bridgeCommand: [fixture.script.path],
        repositoryRoot: fixture.tempRoot
    )

    do {
        _ = try client.notePut(
            vaultRoot: fixture.tempRoot.path,
            dbPath: fixture.tempRoot.appendingPathComponent("obs.sqlite").path,
            path: "notes/mock.md",
            content: "x"
        )
        Issue.record("expected mapped bridge error")
    } catch let error as ObsBridgeClientError {
        switch error {
        case .bridgeError(let typedError):
            switch typedError {
            case .notePutUpdateFailed(let dto):
                #expect(dto.code == "bridge.note_put.update_failed")
                #expect(dto.context["path"] == "notes/mock.md")
            default:
                Issue.record("unexpected typed error: \(typedError)")
            }
        default:
            Issue.record("unexpected client error: \(error)")
        }
    }
}

@Test func bridge_client_maps_unknown_bridge_error_codes_to_unknown_case() throws {
    let fixture = try makeMockBridgeScript(
        payload: """
        {"schema_version":"v1.0","ok":false,"value":null,"error":{"code":"bridge.future.experimental","message":"future","hint":null,"context":{}}}
        """
    )
    defer { try? FileManager.default.removeItem(at: fixture.tempRoot) }

    let client = ObsBridgeClient(
        bridgeCommand: [fixture.script.path],
        repositoryRoot: fixture.tempRoot
    )

    do {
        _ = try client.vaultStats(
            vaultRoot: fixture.tempRoot.path,
            dbPath: fixture.tempRoot.appendingPathComponent("obs.sqlite").path
        )
        Issue.record("expected unknown bridge error mapping")
    } catch let error as ObsBridgeClientError {
        switch error {
        case .bridgeError(let typedError):
            switch typedError {
            case .unknown(let dto):
                #expect(dto.code == "bridge.future.experimental")
                #expect(typedError.bridgeCode == "bridge.future.experimental")
            default:
                Issue.record("unexpected typed error: \(typedError)")
            }
        default:
            Issue.record("unexpected client error: \(error)")
        }
    }
}

private struct MockBridgeScriptFixture {
    let tempRoot: URL
    let script: URL
}

private func makeMockBridgeScript(payload: String) throws -> MockBridgeScriptFixture {
    let fileManager = FileManager.default
    let tempRoot = fileManager.temporaryDirectory
        .appendingPathComponent("obs-bridge-mock-\(UUID().uuidString)")
    try fileManager.createDirectory(at: tempRoot, withIntermediateDirectories: true)

    let script = tempRoot.appendingPathComponent("mock-bridge.sh")
    let body = """
    #!/bin/sh
    cat <<'JSON'
    \(payload)
    JSON
    """
    try body.write(to: script, atomically: true, encoding: .utf8)
    try fileManager.setAttributes([.posixPermissions: NSNumber(value: 0o755)], ofItemAtPath: script.path)
    return MockBridgeScriptFixture(tempRoot: tempRoot, script: script)
}
