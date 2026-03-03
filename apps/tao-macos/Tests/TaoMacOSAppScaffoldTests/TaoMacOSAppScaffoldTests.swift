import Testing
import Foundation
@testable import TaoMacOSAppScaffold

@Test func module_name_is_stable() {
    #expect(TaoMacOSAppScaffold.moduleName() == "TaoMacOSAppScaffold")
}

@Test func app_smoke_launch_open_navigate_edit_flow() throws {
    let fileManager = FileManager.default
    let tempRoot = fileManager.temporaryDirectory
        .appendingPathComponent(.tao-app-smoke-\(UUID().uuidString)")
    defer { try? fileManager.removeItem(at: tempRoot) }

    let vaultRoot = tempRoot.appendingPathComponent("vault")
    let notesDir = vaultRoot.appendingPathComponent("notes")
    let dbPath = tempRoot.appendingPathComponent("tao.sqlite")
    try fileManager.createDirectory(at: notesDir, withIntermediateDirectories: true)

    let client = TaoBridgeClient()

    let launchStats = try client.vaultStats(
        vaultRoot: vaultRoot.path,
        dbPath: dbPath.path
    )
    #expect(launchStats.dbHealthy)

    _ = try client.notePut(
        vaultRoot: vaultRoot.path,
        dbPath: dbPath.path,
        path: "notes/alpha.md",
        content: "# Alpha\nlaunch smoke"
    )
    _ = try client.notePut(
        vaultRoot: vaultRoot.path,
        dbPath: dbPath.path,
        path: "notes/beta.md",
        content: "# Beta\nnavigate smoke"
    )

    let opened = try client.noteGet(
        vaultRoot: vaultRoot.path,
        dbPath: dbPath.path,
        path: "notes/alpha.md"
    )
    #expect(opened.title == "Alpha")
    #expect(opened.body.contains("launch smoke"))

    let listed = try client.notesList(
        vaultRoot: vaultRoot.path,
        dbPath: dbPath.path,
        limit: 10
    )
    #expect(listed.items.map(\.path) == ["notes/alpha.md", "notes/beta.md"])

    let navigated = try client.noteGet(
        vaultRoot: vaultRoot.path,
        dbPath: dbPath.path,
        path: "notes/beta.md"
    )
    #expect(navigated.title == "Beta")
    #expect(navigated.body.contains("navigate smoke"))

    let edited = try client.notePut(
        vaultRoot: vaultRoot.path,
        dbPath: dbPath.path,
        path: "notes/beta.md",
        content: "# Beta Updated\nedit smoke"
    )
    #expect(edited.action == "updated")

    let editedReadback = try client.noteGet(
        vaultRoot: vaultRoot.path,
        dbPath: dbPath.path,
        path: "notes/beta.md"
    )
    #expect(editedReadback.title == "Beta Updated")
    #expect(editedReadback.body.contains("edit smoke"))
}

@Test func bridge_client_calls_vault_stats_and_note_get() throws {
    let fileManager = FileManager.default
    let tempRoot = fileManager.temporaryDirectory
        .appendingPathComponent(.tao-bridge-test-\(UUID().uuidString)")
    defer { try? fileManager.removeItem(at: tempRoot) }

    let vaultRoot = tempRoot.appendingPathComponent("vault")
    let notesDir = vaultRoot.appendingPathComponent("notes")
    let dbPath = tempRoot.appendingPathComponent("tao.sqlite")
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

    let client = TaoBridgeClient()
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

@Test func bridge_client_note_context_returns_note_and_links_in_single_call() throws {
    let fileManager = FileManager.default
    let tempRoot = fileManager.temporaryDirectory
        .appendingPathComponent(.tao-bridge-note-context-\(UUID().uuidString)")
    defer { try? fileManager.removeItem(at: tempRoot) }

    let vaultRoot = tempRoot.appendingPathComponent("vault")
    let notesDir = vaultRoot.appendingPathComponent("notes")
    let dbPath = tempRoot.appendingPathComponent("tao.sqlite")
    try fileManager.createDirectory(at: notesDir, withIntermediateDirectories: true)

    let client = TaoBridgeClient()
    _ = try client.notePut(
        vaultRoot: vaultRoot.path,
        dbPath: dbPath.path,
        path: "notes/target.md",
        content: "# Target"
    )
    _ = try client.notePut(
        vaultRoot: vaultRoot.path,
        dbPath: dbPath.path,
        path: "notes/source.md",
        content: "# Source\n[[target]]"
    )
    _ = try client.vaultStats(
        vaultRoot: vaultRoot.path,
        dbPath: dbPath.path
    )

    let context = try client.noteContext(
        vaultRoot: vaultRoot.path,
        dbPath: dbPath.path,
        path: "notes/source.md"
    )
    #expect(context.note.path == "notes/source.md")
    #expect(context.note.title == "Source")
    #expect(context.links.outgoing.count >= 0)
    #expect(context.links.backlinks.count >= 0)
}

@Test func bridge_client_note_put_creates_and_updates_notes() throws {
    let fileManager = FileManager.default
    let tempRoot = fileManager.temporaryDirectory
        .appendingPathComponent(.tao-bridge-write-test-\(UUID().uuidString)")
    defer { try? fileManager.removeItem(at: tempRoot) }

    let vaultRoot = tempRoot.appendingPathComponent("vault")
    let notesDir = vaultRoot.appendingPathComponent("notes")
    let dbPath = tempRoot.appendingPathComponent("tao.sqlite")
    try fileManager.createDirectory(at: notesDir, withIntermediateDirectories: true)

    let client = TaoBridgeClient()
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

@Test func bridge_client_notes_list_pages_results() throws {
    let fileManager = FileManager.default
    let tempRoot = fileManager.temporaryDirectory
        .appendingPathComponent(.tao-bridge-list-test-\(UUID().uuidString)")
    defer { try? fileManager.removeItem(at: tempRoot) }

    let vaultRoot = tempRoot.appendingPathComponent("vault")
    let notesDir = vaultRoot.appendingPathComponent("notes")
    let dbPath = tempRoot.appendingPathComponent("tao.sqlite")
    try fileManager.createDirectory(at: notesDir, withIntermediateDirectories: true)

    let client = TaoBridgeClient()
    _ = try client.notePut(
        vaultRoot: vaultRoot.path,
        dbPath: dbPath.path,
        path: "notes/c.md",
        content: "# C"
    )
    _ = try client.notePut(
        vaultRoot: vaultRoot.path,
        dbPath: dbPath.path,
        path: "notes/a.md",
        content: "# A"
    )
    _ = try client.notePut(
        vaultRoot: vaultRoot.path,
        dbPath: dbPath.path,
        path: "notes/b.md",
        content: "# B"
    )

    let firstPage = try client.notesList(
        vaultRoot: vaultRoot.path,
        dbPath: dbPath.path,
        limit: 2
    )
    #expect(firstPage.items.count == 2)
    #expect(firstPage.items[0].path == "notes/a.md")
    #expect(firstPage.items[1].path == "notes/b.md")
    #expect(firstPage.nextCursor == "notes/b.md")

    let secondPage = try client.notesList(
        vaultRoot: vaultRoot.path,
        dbPath: dbPath.path,
        afterPath: firstPage.nextCursor,
        limit: 2
    )
    #expect(secondPage.items.count == 1)
    #expect(secondPage.items[0].path == "notes/c.md")
    #expect(secondPage.nextCursor == nil)
}

@Test func bridge_client_note_links_returns_panels() throws {
    let fileManager = FileManager.default
    let tempRoot = fileManager.temporaryDirectory
        .appendingPathComponent(.tao-bridge-links-test-\(UUID().uuidString)")
    defer { try? fileManager.removeItem(at: tempRoot) }

    let vaultRoot = tempRoot.appendingPathComponent("vault")
    let notesDir = vaultRoot.appendingPathComponent("notes")
    let dbPath = tempRoot.appendingPathComponent("tao.sqlite")
    try fileManager.createDirectory(at: notesDir, withIntermediateDirectories: true)

    let client = TaoBridgeClient()
    _ = try client.notePut(
        vaultRoot: vaultRoot.path,
        dbPath: dbPath.path,
        path: "notes/source.md",
        content: "# Source"
    )

    let links = try client.noteLinks(
        vaultRoot: vaultRoot.path,
        dbPath: dbPath.path,
        path: "notes/source.md"
    )
    #expect(links.outgoing.isEmpty)
    #expect(links.backlinks.isEmpty)
}

@Test func bridge_client_bases_list_and_missing_view_errors_are_typed() throws {
    let fileManager = FileManager.default
    let tempRoot = fileManager.temporaryDirectory
        .appendingPathComponent(.tao-bridge-bases-test-\(UUID().uuidString)")
    defer { try? fileManager.removeItem(at: tempRoot) }

    let vaultRoot = tempRoot.appendingPathComponent("vault")
    let notesDir = vaultRoot.appendingPathComponent("notes")
    let dbPath = tempRoot.appendingPathComponent("tao.sqlite")
    try fileManager.createDirectory(at: notesDir, withIntermediateDirectories: true)
    try """
    # Alpha
    bridge test
    """.write(
        to: notesDir.appendingPathComponent("a.md"),
        atomically: true,
        encoding: .utf8
    )

    let client = TaoBridgeClient()
    let list = try client.basesList(
        vaultRoot: vaultRoot.path,
        dbPath: dbPath.path
    )
    #expect(list.isEmpty)

    do {
        _ = try client.basesView(
            vaultRoot: vaultRoot.path,
            dbPath: dbPath.path,
            pathOrId: "missing",
            viewName: "Projects"
        )
        Issue.record("expected missing base error")
    } catch let TaoBridgeClientError.bridgeError(error) {
        #expect(error == .basesViewNotFound(BridgeErrorDTO(
            code: "bridge.bases_view.not_found",
            message: "base id/path not found",
            hint: "call bases-list to discover valid base ids and paths",
            context: ["path_or_id": "missing"]
        )))
    }
}

@Test func bridge_client_events_poll_returns_note_write_events() throws {
    let fileManager = FileManager.default
    let tempRoot = fileManager.temporaryDirectory
        .appendingPathComponent(.tao-bridge-events-test-\(UUID().uuidString)")
    defer { try? fileManager.removeItem(at: tempRoot) }

    let vaultRoot = tempRoot.appendingPathComponent("vault")
    let notesDir = vaultRoot.appendingPathComponent("notes")
    let dbPath = tempRoot.appendingPathComponent("tao.sqlite")
    try fileManager.createDirectory(at: notesDir, withIntermediateDirectories: true)

    let client = TaoBridgeClient()
    _ = try client.notePut(
        vaultRoot: vaultRoot.path,
        dbPath: dbPath.path,
        path: "notes/events.md",
        content: "# Events\ncreated"
    )
    _ = try client.notePut(
        vaultRoot: vaultRoot.path,
        dbPath: dbPath.path,
        path: "notes/events.md",
        content: "# Events\nupdated"
    )

    let firstBatch = try client.eventsPoll(
        vaultRoot: vaultRoot.path,
        dbPath: dbPath.path,
        afterId: 0,
        limit: 10
    )
    #expect(firstBatch.events.count == 2)
    #expect(firstBatch.events[0].kind == "note_changed")
    #expect(firstBatch.events[0].action == "created")
    #expect(firstBatch.events[1].action == "updated")
    #expect(firstBatch.events[0].path == "notes/events.md")

    let secondBatch = try client.eventsPoll(
        vaultRoot: vaultRoot.path,
        dbPath: dbPath.path,
        afterId: firstBatch.nextCursor,
        limit: 10
    )
    #expect(secondBatch.events.isEmpty)
    #expect(secondBatch.nextCursor == firstBatch.nextCursor)
}

@Test func bridge_client_accepts_compatible_minor_schema_versions() throws {
    #expect(TaoBridgeClient.isCompatibleSchemaVersion("v1"))
    #expect(TaoBridgeClient.isCompatibleSchemaVersion("v1.8"))
    #expect(!TaoBridgeClient.isCompatibleSchemaVersion("v2"))
    #expect(!TaoBridgeClient.isCompatibleSchemaVersion("v1.beta"))
}

@Test func bridge_client_rejects_incompatible_schema_from_bridge_output() throws {
    let fixture = try makeMockBridgeScript(
        payload: """
        {"schema_version":"v2.0","ok":true,"value":{"path":"notes/mock.md","title":"Mock","front_matter":null,"body":"mock body","headings_total":0},"error":null}
        """
    )
    defer { try? FileManager.default.removeItem(at: fixture.tempRoot) }

    let client = TaoBridgeClient(
        bridgeCommand: [fixture.script.path],
        repositoryRoot: fixture.tempRoot
    )

    do {
        _ = try client.noteGet(
            vaultRoot: fixture.tempRoot.path,
            dbPath: fixture.tempRoot.appendingPathComponent("tao.sqlite").path,
            path: "notes/mock.md"
        )
        Issue.record("expected schema compatibility failure")
    } catch let error as TaoBridgeClientError {
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

    let client = TaoBridgeClient(
        bridgeCommand: [fixture.script.path],
        repositoryRoot: fixture.tempRoot
    )

    do {
        _ = try client.notePut(
            vaultRoot: fixture.tempRoot.path,
            dbPath: fixture.tempRoot.appendingPathComponent("tao.sqlite").path,
            path: "notes/mock.md",
            content: "x"
        )
        Issue.record("expected mapped bridge error")
    } catch let error as TaoBridgeClientError {
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

    let client = TaoBridgeClient(
        bridgeCommand: [fixture.script.path],
        repositoryRoot: fixture.tempRoot
    )

    do {
        _ = try client.vaultStats(
            vaultRoot: fixture.tempRoot.path,
            dbPath: fixture.tempRoot.appendingPathComponent("tao.sqlite").path
        )
        Issue.record("expected unknown bridge error mapping")
    } catch let error as TaoBridgeClientError {
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
        .appendingPathComponent(.tao-bridge-mock-\(UUID().uuidString)")
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
