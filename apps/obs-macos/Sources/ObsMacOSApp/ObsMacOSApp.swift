import SwiftUI
import AppKit
import ObsMacOSAppScaffold

@main
struct ObsMacOSApp: App {
    @State private var quickOpenCommandNonce = 0

    var body: some Scene {
        WindowGroup {
            ObsRootSplitView(quickOpenCommandNonce: $quickOpenCommandNonce)
                .frame(minWidth: 1120, minHeight: 720)
        }
        .commands {
            CommandMenu("Navigate") {
                Button("Quick Open Note") {
                    quickOpenCommandNonce &+= 1
                }
                .keyboardShortcut("k", modifiers: [.command])
            }
        }
    }
}

private enum SidebarItem: String, CaseIterable, Identifiable {
    case vault = "Vault"
    case notes = "Notes"
    case bases = "Bases"

    var id: String { rawValue }
}

private enum AppRecoveryAction {
    case retryLoadVaultStats
    case retryLoadSelectedNote(path: String)
    case retrySaveProperties
    case retryLoadLinks(path: String)
    case retryLoadBasesList
    case retryLoadBasePage

    var label: String {
        switch self {
        case .retryLoadVaultStats:
            return "Retry Vault Stats"
        case .retryLoadSelectedNote:
            return "Retry Note Load"
        case .retrySaveProperties:
            return "Retry Save"
        case .retryLoadLinks:
            return "Retry Links"
        case .retryLoadBasesList:
            return "Retry Bases Load"
        case .retryLoadBasePage:
            return "Retry Page Load"
        }
    }
}

private struct AppErrorState: Identifiable {
    let id = UUID()
    let title: String
    let message: String
    let bridgeCode: String?
    let hint: String?
    let context: [String: String]
    let recoveryAction: AppRecoveryAction?
}

private enum StartupPersistenceKeys {
    static let vaultRoot = "obs.startup.vault_root"
    static let dbPath = "obs.startup.db_path"
    static let notePath = "obs.startup.note_path"
}

private struct ObsRootSplitView: View {
    @Environment(\.accessibilityReduceMotion) private var reduceMotion
    @Binding var quickOpenCommandNonce: Int
    @State private var selectedSidebarItem: SidebarItem? = .notes
    @State private var selectedTreePath: String?
    @State private var vaultRoot = ""
    @State private var dbPath = ""
    @State private var openedVaultRoot: String?
    @State private var hasRestoredStartupState = false
    @State private var isRestoringStartupState = false
    @State private var pendingRestoredNotePath: String?
    @State private var statsSummary = "Bridge read APIs not called yet."
    @State private var bridgeError: String?
    @State private var appErrorState: AppErrorState?
    @State private var isLoadingStats = false
    @State private var selectedNote: BridgeNoteView?
    @State private var noteError: String?
    @State private var isLoadingNote = false
    @State private var frontMatterDraft = ""
    @State private var propertiesStatus: String?
    @State private var propertiesError: String?
    @State private var isSavingProperties = false
    @State private var linkPanels: BridgeLinkPanels?
    @State private var linksError: String?
    @State private var isLoadingLinks = false
    @State private var isQuickOpenPresented = false
    @State private var quickOpenQuery = ""
    @FocusState private var isQuickOpenQueryFocused: Bool
    @State private var baseRefs: [BridgeBaseRef] = []
    @State private var selectedBaseId: String?
    @State private var selectedBaseViewName: String?
    @State private var baseTablePage: BridgeBaseTablePage?
    @State private var basePageNumber: UInt32 = 1
    @State private var basePageSize: UInt32 = 50
    @State private var isLoadingBases = false
    @State private var isLoadingBasePage = false
    @State private var basesError: String?
    @StateObject private var fileTreeViewModel = FileTreeViewModel()

    private var quickOpenResults: [BridgeNoteSummary] {
        fileTreeViewModel.quickOpenMatches(query: quickOpenQuery, limit: 40)
    }

    private var selectedBaseRef: BridgeBaseRef? {
        guard let selectedBaseId else {
            return nil
        }
        return baseRefs.first(where: { $0.baseId == selectedBaseId })
    }

    private var visibleBaseColumns: [BridgeBaseColumn] {
        guard let baseTablePage else {
            return []
        }
        return baseTablePage.columns.filter { !$0.hidden }
    }

    var body: some View {
        NavigationSplitView {
            List(SidebarItem.allCases, selection: $selectedSidebarItem) { item in
                Label(item.rawValue, systemImage: icon(for: item))
                    .tag(Optional(item))
            }
            .navigationTitle("obs")
        } content: {
            VStack(alignment: .leading, spacing: 16) {
                Text("Workspace")
                    .font(.headline)
                Text(contentLabel(for: selectedSidebarItem))
                    .foregroundStyle(.secondary)
                if let appErrorState {
                    appErrorBanner(appErrorState)
                }
                workspacePane
                Divider()
                Text("Bridge Integration")
                    .font(.headline)
                HStack(spacing: 12) {
                    Button("Choose Vault...") {
                        openVaultFromPicker()
                    }

                    if let openedVaultRoot {
                        Text("Opened: \(openedVaultRoot)")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                            .lineLimit(1)
                    }
                }
                TextField("/absolute/path/to/vault", text: $vaultRoot)
                    .textFieldStyle(.roundedBorder)
                TextField("/absolute/path/to/obs.sqlite", text: $dbPath)
                    .textFieldStyle(.roundedBorder)
                HStack(spacing: 12) {
                    Button("Load Vault Stats") {
                        loadVaultStats()
                    }
                    .disabled(isLoadingStats)

                    if isLoadingStats {
                        ProgressView()
                            .controlSize(.small)
                    }
                }

                Text(statsSummary)
                    .font(.callout)
                    .foregroundStyle(.secondary)

                if let bridgeError {
                    Text(bridgeError)
                        .font(.callout)
                        .foregroundStyle(.red)
                }
                Divider()
                Text("Bridge Module: \(ObsMacOSAppScaffold.moduleName())")
                    .font(.caption)
                    .foregroundStyle(.tertiary)
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            .padding(20)
        } detail: {
            VStack(alignment: .leading, spacing: 12) {
                Text("Inspector")
                    .font(.headline)
                if isLoadingNote {
                    ProgressView("Loading note...")
                } else if let selectedNote {
                    ScrollView {
                        VStack(alignment: .leading, spacing: 12) {
                            Text(selectedNote.title)
                                .font(.title3.weight(.semibold))
                            Text(.init(selectedNote.body))
                                .textSelection(.enabled)
                                .frame(maxWidth: .infinity, alignment: .leading)
                            Divider()
                            Text("Properties")
                                .font(.headline)

                            if parsedFrontMatter(from: frontMatterDraft).isEmpty {
                                Text("No parsed properties")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            } else {
                                ForEach(parsedFrontMatter(from: frontMatterDraft), id: \.key) { entry in
                                    HStack {
                                        Text(entry.key)
                                            .font(.caption.weight(.semibold))
                                        Spacer()
                                        Text(entry.value)
                                            .font(.caption.monospaced())
                                            .foregroundStyle(.secondary)
                                    }
                                }
                            }

                            TextEditor(text: $frontMatterDraft)
                                .font(.system(.caption, design: .monospaced))
                                .frame(minHeight: 140)
                                .border(.quaternary)

                            HStack(spacing: 12) {
                                Button("Save Properties") {
                                    saveProperties()
                                }
                                .disabled(isSavingProperties)

                                if isSavingProperties {
                                    ProgressView()
                                        .controlSize(.small)
                                }
                            }

                            if let propertiesStatus {
                                Text(propertiesStatus)
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            }
                            if let propertiesError {
                                Text(propertiesError)
                                    .font(.caption)
                                    .foregroundStyle(.red)
                            }

                            Divider()
                            Text("Links")
                                .font(.headline)

                            if isLoadingLinks {
                                ProgressView("Loading links...")
                            } else if let linkPanels {
                                HStack(alignment: .top, spacing: 24) {
                                    VStack(alignment: .leading, spacing: 8) {
                                        Text("Outgoing")
                                            .font(.caption.weight(.semibold))
                                        if linkPanels.outgoing.isEmpty {
                                            Text("none")
                                                .font(.caption)
                                                .foregroundStyle(.secondary)
                                        } else {
                                            ForEach(Array(linkPanels.outgoing.enumerated()), id: \.offset) { _, link in
                                                Text(link.targetPath ?? link.sourcePath)
                                                    .font(.caption)
                                                    .foregroundStyle(.secondary)
                                            }
                                        }
                                    }

                                    VStack(alignment: .leading, spacing: 8) {
                                        Text("Backlinks")
                                            .font(.caption.weight(.semibold))
                                        if linkPanels.backlinks.isEmpty {
                                            Text("none")
                                                .font(.caption)
                                                .foregroundStyle(.secondary)
                                        } else {
                                            ForEach(Array(linkPanels.backlinks.enumerated()), id: \.offset) { _, link in
                                                Text(link.sourcePath)
                                                    .font(.caption)
                                                    .foregroundStyle(.secondary)
                                            }
                                        }
                                    }
                                }
                            }

                            if let linksError {
                                Text(linksError)
                                    .font(.caption)
                                    .foregroundStyle(.red)
                            }
                        }
                        .frame(maxWidth: .infinity, alignment: .leading)
                    }
                } else if let noteError {
                    Text(noteError)
                        .foregroundStyle(.red)
                } else {
                    if let selectedTreePath {
                        Text("Selected: \(selectedTreePath)")
                            .foregroundStyle(.secondary)
                    } else {
                        Text("Select a file from the tree to inspect its note.")
                            .foregroundStyle(.secondary)
                    }
                }

                if let selectedNote {
                    Divider()
                    Text("Path: \(selectedNote.path)")
                        .foregroundStyle(.secondary)
                }
                Spacer()
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            .padding(20)
        }
        .animation(reduceMotion ? nil : .easeOut(duration: 0.18), value: selectedSidebarItem)
        .animation(reduceMotion ? nil : .easeOut(duration: 0.18), value: selectedTreePath)
        .animation(reduceMotion ? nil : .easeOut(duration: 0.18), value: appErrorState?.id)
        .transaction { transaction in
            if reduceMotion {
                transaction.disablesAnimations = true
            }
        }
        .onAppear {
            restoreStartupStateIfNeeded()
        }
        .onChange(of: selectedTreePath) { _, newValue in
            loadSelectedNote(path: newValue)
            persistStartupState(notePathOverride: newValue)
        }
        .onChange(of: quickOpenCommandNonce) { _, _ in
            presentQuickOpen()
        }
        .onChange(of: selectedSidebarItem) { _, newValue in
            if newValue == .bases && openedVaultRoot != nil && baseRefs.isEmpty && !isLoadingBases {
                loadBasesList()
            }
        }
        .onChange(of: selectedBaseId) { _, newValue in
            guard let newValue, let base = baseRefs.first(where: { $0.baseId == newValue }) else {
                selectedBaseViewName = nil
                baseTablePage = nil
                return
            }

            if selectedBaseViewName == nil || !base.views.contains(selectedBaseViewName ?? "") {
                selectedBaseViewName = base.views.first
            } else {
                basePageNumber = 1
                loadSelectedBasePage()
            }
        }
        .onChange(of: selectedBaseViewName) { oldValue, newValue in
            guard oldValue != newValue else {
                return
            }
            guard newValue != nil else {
                baseTablePage = nil
                return
            }
            basePageNumber = 1
            loadSelectedBasePage()
        }
        .sheet(isPresented: $isQuickOpenPresented) {
            VStack(alignment: .leading, spacing: 12) {
                Text("Quick Open")
                    .font(.headline)

                Text("Search loaded notes by title or path.")
                    .font(.caption)
                    .foregroundStyle(.secondary)

                TextField("Type to search...", text: $quickOpenQuery)
                    .textFieldStyle(.roundedBorder)
                    .focused($isQuickOpenQueryFocused)
                    .onSubmit {
                        if let firstMatch = quickOpenResults.first {
                            openNoteFromQuickOpen(path: firstMatch.path)
                        }
                    }

                if !fileTreeViewModel.hasLoadedNotes {
                    Text("No notes loaded yet. Open a vault and load notes to enable quick open.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                } else if quickOpenResults.isEmpty {
                    Text("No matches for \"\(quickOpenQuery)\"")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                } else {
                    List(quickOpenResults, id: \.path) { summary in
                        Button {
                            openNoteFromQuickOpen(path: summary.path)
                        } label: {
                            VStack(alignment: .leading, spacing: 2) {
                                Text(summary.title.isEmpty ? summary.path : summary.title)
                                    .lineLimit(1)
                                Text(summary.path)
                                    .font(.caption.monospaced())
                                    .foregroundStyle(.secondary)
                                    .lineLimit(1)
                            }
                            .frame(maxWidth: .infinity, alignment: .leading)
                            .contentShape(Rectangle())
                        }
                        .buttonStyle(.plain)
                    }
                    .listStyle(.plain)
                }

                Spacer(minLength: 0)
            }
            .padding(20)
            .frame(minWidth: 680, minHeight: 420)
            .onAppear {
                isQuickOpenQueryFocused = true
            }
        }
    }

    private func icon(for item: SidebarItem) -> String {
        switch item {
        case .vault:
            return "folder"
        case .notes:
            return "doc.text"
        case .bases:
            return "tablecells"
        }
    }

    private func contentLabel(for item: SidebarItem?) -> String {
        switch item {
        case .vault:
            return "Vault overview"
        case .notes:
            return "Lazy-loaded file tree"
        case .bases:
            return "Base table views"
        case .none:
            return "Select a section"
        }
    }

    @ViewBuilder
    private func appErrorBanner(_ error: AppErrorState) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            Text(error.title)
                .font(.caption.weight(.semibold))
                .foregroundStyle(.red)
            Text(error.message)
                .font(.caption)
                .foregroundStyle(.primary)

            if let bridgeCode = error.bridgeCode {
                Text("code: \(bridgeCode)")
                    .font(.caption2.monospaced())
                    .foregroundStyle(.secondary)
            }
            if let hint = error.hint, !hint.isEmpty {
                Text("hint: \(hint)")
                    .font(.caption2)
                    .foregroundStyle(.secondary)
            }
            if !error.context.isEmpty {
                Text(
                    error.context
                        .sorted(by: { $0.key < $1.key })
                        .map { "\($0.key)=\($0.value)" }
                        .joined(separator: ", ")
                )
                .font(.caption2.monospaced())
                .foregroundStyle(.secondary)
                .lineLimit(2)
            }

            HStack(spacing: 8) {
                if let recoveryAction = error.recoveryAction {
                    Button(recoveryAction.label) {
                        performRecoveryAction(recoveryAction)
                    }
                    .buttonStyle(.bordered)
                    .controlSize(.small)
                }
                Button("Dismiss") {
                    appErrorState = nil
                }
                .buttonStyle(.bordered)
                .controlSize(.small)
            }
        }
        .padding(10)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(
            RoundedRectangle(cornerRadius: 8)
                .fill(Color.red.opacity(0.08))
        )
    }

    @ViewBuilder
    private var workspacePane: some View {
        switch selectedSidebarItem {
        case .notes:
            List(selection: $selectedTreePath) {
                OutlineGroup(fileTreeViewModel.roots, children: \.children) { node in
                    HStack(spacing: 8) {
                        Image(systemName: node.isFile ? "doc.text" : "folder")
                            .foregroundStyle(node.isFile ? .secondary : .primary)
                        Text(node.name)
                    }
                    .tag(node.path)
                }
            }
            .frame(minHeight: 240)

            HStack(spacing: 12) {
                Button("Load More Notes") {
                    fileTreeViewModel.loadNextPage()
                }
                .disabled(!fileTreeViewModel.canLoadMore || fileTreeViewModel.isLoading)

                Button("Quick Open...") {
                    presentQuickOpen()
                }
                .disabled(openedVaultRoot == nil)

                if fileTreeViewModel.isLoading {
                    ProgressView()
                        .controlSize(.small)
                }
            }

            if let errorMessage = fileTreeViewModel.errorMessage {
                Text(errorMessage)
                    .font(.caption)
                    .foregroundStyle(.red)
            }
        case .vault:
            Text(openedVaultRoot ?? "No vault open")
                .font(.callout)
                .foregroundStyle(.secondary)
                .frame(maxWidth: .infinity, alignment: .leading)
        case .bases:
            VStack(alignment: .leading, spacing: 12) {
                if openedVaultRoot == nil {
                    Text("Open a vault before loading bases.")
                        .font(.callout)
                        .foregroundStyle(.secondary)
                } else {
                    HStack(spacing: 12) {
                        Button("Load Bases") {
                            loadBasesList()
                        }
                        .disabled(isLoadingBases || isLoadingBasePage)

                        if isLoadingBases {
                            ProgressView()
                                .controlSize(.small)
                        }

                        if let baseTablePage {
                            Text(basePaginationSummary(for: baseTablePage))
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                    }

                    if let basesError {
                        Text(basesError)
                            .font(.caption)
                            .foregroundStyle(.red)
                    }

                    if baseRefs.isEmpty {
                        Text("No indexed bases found.")
                            .font(.callout)
                            .foregroundStyle(.secondary)
                    } else {
                        Picker("Base", selection: $selectedBaseId) {
                            ForEach(baseRefs, id: \.baseId) { base in
                                Text(base.filePath)
                                    .tag(Optional(base.baseId))
                            }
                        }
                        .pickerStyle(.menu)

                        if let selectedBaseRef {
                            Picker("View", selection: $selectedBaseViewName) {
                                ForEach(selectedBaseRef.views, id: \.self) { viewName in
                                    Text(viewName)
                                        .tag(Optional(viewName))
                                }
                            }
                            .pickerStyle(.segmented)
                        }

                        HStack(spacing: 12) {
                            Button("Previous Page") {
                                if basePageNumber > 1 {
                                    basePageNumber -= 1
                                    loadSelectedBasePage()
                                }
                            }
                            .disabled(basePageNumber <= 1 || isLoadingBasePage)

                            Button("Next Page") {
                                guard baseTablePage?.hasMore == true else {
                                    return
                                }
                                basePageNumber += 1
                                loadSelectedBasePage()
                            }
                            .disabled(!(baseTablePage?.hasMore ?? false) || isLoadingBasePage)

                            if isLoadingBasePage {
                                ProgressView()
                                    .controlSize(.small)
                            }
                        }

                        if let baseTablePage {
                            Table(baseTablePage.rows) {
                                TableColumn("Path", value: \.filePath)
                                TableColumn("Values") { row in
                                    Text(baseRowSummary(row))
                                        .lineLimit(1)
                                }
                            }
                            .frame(minHeight: 240)
                        } else if !isLoadingBasePage {
                            Text("Select a base and view to load table rows.")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                    }
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
            .onAppear {
                if openedVaultRoot != nil && baseRefs.isEmpty && !isLoadingBases {
                    loadBasesList()
                }
            }
        case .none:
            EmptyView()
        }
    }

    private func restoreStartupStateIfNeeded() {
        guard !hasRestoredStartupState else {
            return
        }
        hasRestoredStartupState = true

        let defaults = UserDefaults.standard
        guard
            let persistedVaultRoot = defaults.string(forKey: StartupPersistenceKeys.vaultRoot),
            let persistedDbPath = defaults.string(forKey: StartupPersistenceKeys.dbPath)
        else {
            return
        }

        let restoredVaultRoot = persistedVaultRoot.trimmingCharacters(in: .whitespacesAndNewlines)
        let restoredDbPath = persistedDbPath.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !restoredVaultRoot.isEmpty, !restoredDbPath.isEmpty else {
            clearStartupState()
            return
        }
        guard FileManager.default.fileExists(atPath: restoredVaultRoot) else {
            clearStartupState()
            return
        }

        vaultRoot = restoredVaultRoot
        dbPath = restoredDbPath
        if let persistedNotePath = defaults.string(forKey: StartupPersistenceKeys.notePath) {
            let trimmed = persistedNotePath.trimmingCharacters(in: .whitespacesAndNewlines)
            if !trimmed.isEmpty {
                pendingRestoredNotePath = trimmed
            }
        }

        isRestoringStartupState = true
        loadVaultStats()
    }

    private func persistStartupState(notePathOverride: String? = nil) {
        let persistedVaultRoot = (openedVaultRoot ?? vaultRoot)
            .trimmingCharacters(in: .whitespacesAndNewlines)
        let persistedDbPath = dbPath.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !persistedVaultRoot.isEmpty, !persistedDbPath.isEmpty else {
            clearStartupState()
            return
        }

        let defaults = UserDefaults.standard
        defaults.set(persistedVaultRoot, forKey: StartupPersistenceKeys.vaultRoot)
        defaults.set(persistedDbPath, forKey: StartupPersistenceKeys.dbPath)

        let persistedNotePath = (notePathOverride ?? selectedTreePath)?
            .trimmingCharacters(in: .whitespacesAndNewlines)
        if let persistedNotePath, !persistedNotePath.isEmpty {
            defaults.set(persistedNotePath, forKey: StartupPersistenceKeys.notePath)
        } else {
            defaults.removeObject(forKey: StartupPersistenceKeys.notePath)
        }
    }

    private func clearStartupState() {
        let defaults = UserDefaults.standard
        defaults.removeObject(forKey: StartupPersistenceKeys.vaultRoot)
        defaults.removeObject(forKey: StartupPersistenceKeys.dbPath)
        defaults.removeObject(forKey: StartupPersistenceKeys.notePath)
    }

    private func performRecoveryAction(_ action: AppRecoveryAction) {
        appErrorState = nil
        switch action {
        case .retryLoadVaultStats:
            loadVaultStats()
        case .retryLoadSelectedNote(let path):
            loadSelectedNote(path: path)
        case .retrySaveProperties:
            saveProperties()
        case .retryLoadLinks(let path):
            loadLinkPanels(path: path)
        case .retryLoadBasesList:
            loadBasesList()
        case .retryLoadBasePage:
            loadSelectedBasePage()
        }
    }

    private func presentAppError(
        _ error: Error,
        operation: String,
        recoveryAction: AppRecoveryAction?
    ) {
        if let clientError = error as? ObsBridgeClientError {
            switch clientError {
            case .bridgeError(let typedError):
                appErrorState = AppErrorState(
                    title: "\(operation) failed",
                    message: typedError.description,
                    bridgeCode: typedError.bridgeCode,
                    hint: typedError.bridgeHint,
                    context: typedError.bridgeContext,
                    recoveryAction: recoveryAction
                )
            default:
                appErrorState = AppErrorState(
                    title: "\(operation) failed",
                    message: clientError.description,
                    bridgeCode: nil,
                    hint: nil,
                    context: [:],
                    recoveryAction: recoveryAction
                )
            }
            return
        }

        appErrorState = AppErrorState(
            title: "\(operation) failed",
            message: String(describing: error),
            bridgeCode: nil,
            hint: nil,
            context: [:],
            recoveryAction: recoveryAction
        )
    }

    private func loadVaultStats() {
        let root = vaultRoot.trimmingCharacters(in: .whitespacesAndNewlines)
        let db = dbPath.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !root.isEmpty, !db.isEmpty else {
            bridgeError = "Provide absolute vault and sqlite paths before loading stats."
            return
        }

        bridgeError = nil
        isLoadingStats = true

        Task {
            do {
                let stats = try await Task.detached(priority: .userInitiated) {
                    try ObsBridgeClient().vaultStats(vaultRoot: root, dbPath: db)
                }.value

                await MainActor.run {
                    statsSummary =
                        "files=\(stats.filesTotal) markdown=\(stats.markdownFiles) dbHealthy=\(stats.dbHealthy)"
                    openedVaultRoot = root
                    fileTreeViewModel.bindVault(vaultRoot: root, dbPath: db)
                    let restoredNotePath = pendingRestoredNotePath
                    pendingRestoredNotePath = nil
                    selectedTreePath = restoredNotePath
                    selectedNote = nil
                    noteError = nil
                    propertiesStatus = nil
                    propertiesError = nil
                    linkPanels = nil
                    linksError = nil
                    baseRefs = []
                    selectedBaseId = nil
                    selectedBaseViewName = nil
                    baseTablePage = nil
                    basePageNumber = 1
                    basesError = nil
                    isLoadingBases = false
                    isLoadingBasePage = false
                    appErrorState = nil
                    isRestoringStartupState = false
                    isLoadingStats = false
                    persistStartupState(notePathOverride: restoredNotePath)
                    if selectedSidebarItem == .bases {
                        loadBasesList()
                    }
                }
            } catch {
                await MainActor.run {
                    bridgeError = "bridge read failed: \(error)"
                    presentAppError(
                        error,
                        operation: "Load vault stats",
                        recoveryAction: .retryLoadVaultStats
                    )
                    if isRestoringStartupState {
                        clearStartupState()
                        pendingRestoredNotePath = nil
                    }
                    isRestoringStartupState = false
                    isLoadingStats = false
                }
            }
        }
    }

    private func openVaultFromPicker() {
        let panel = NSOpenPanel()
        panel.title = "Open Vault"
        panel.prompt = "Open"
        panel.canChooseFiles = false
        panel.canChooseDirectories = true
        panel.allowsMultipleSelection = false
        panel.canCreateDirectories = false

        guard panel.runModal() == .OK, let url = panel.url else {
            return
        }

        vaultRoot = url.path
        dbPath = url.appendingPathComponent(".obs.sqlite").path
        loadVaultStats()
    }

    private func loadSelectedNote(path: String?) {
        guard let path, path.hasSuffix(".md") else {
            selectedNote = nil
            noteError = nil
            linkPanels = nil
            linksError = nil
            return
        }

        let root = vaultRoot.trimmingCharacters(in: .whitespacesAndNewlines)
        let db = dbPath.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !root.isEmpty, !db.isEmpty else {
            noteError = "open a vault before reading notes"
            return
        }

        isLoadingNote = true
        noteError = nil

        Task {
            do {
                let context = try await Task.detached(priority: .userInitiated) {
                    try ObsBridgeClient().noteContext(vaultRoot: root, dbPath: db, path: path)
                }.value
                await MainActor.run {
                    selectedNote = context.note
                    frontMatterDraft = context.note.frontMatter ?? ""
                    propertiesStatus = nil
                    propertiesError = nil
                    linkPanels = context.links
                    linksError = nil
                    appErrorState = nil
                    isLoadingNote = false
                    persistStartupState(notePathOverride: context.note.path)
                }
            } catch {
                await MainActor.run {
                    selectedNote = nil
                    noteError = "note read failed: \(error)"
                    presentAppError(
                        error,
                        operation: "Load note",
                        recoveryAction: .retryLoadSelectedNote(path: path)
                    )
                    propertiesStatus = nil
                    propertiesError = nil
                    linkPanels = nil
                    linksError = nil
                    isLoadingNote = false
                }
            }
        }
    }

    private func parsedFrontMatter(from raw: String) -> [(key: String, value: String)] {
        raw
            .split(separator: "\n")
            .compactMap { line in
                let pieces = line.split(separator: ":", maxSplits: 1).map(String.init)
                guard pieces.count == 2 else {
                    return nil
                }
                return (
                    key: pieces[0].trimmingCharacters(in: .whitespacesAndNewlines),
                    value: pieces[1].trimmingCharacters(in: .whitespacesAndNewlines)
                )
            }
            .filter { !$0.key.isEmpty }
    }

    private func saveProperties() {
        guard let selectedNote else {
            propertiesError = "select a note before saving properties"
            return
        }

        let root = vaultRoot.trimmingCharacters(in: .whitespacesAndNewlines)
        let db = dbPath.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !root.isEmpty, !db.isEmpty else {
            propertiesError = "open a vault before saving properties"
            return
        }

        let trimmedFrontMatter = frontMatterDraft.trimmingCharacters(in: .whitespacesAndNewlines)
        let content: String
        if trimmedFrontMatter.isEmpty {
            content = selectedNote.body
        } else {
            content = "---\n\(trimmedFrontMatter)\n---\n\(selectedNote.body)"
        }

        isSavingProperties = true
        propertiesError = nil
        propertiesStatus = nil

        Task {
            do {
                _ = try await Task.detached(priority: .userInitiated) {
                    try ObsBridgeClient().notePut(
                        vaultRoot: root,
                        dbPath: db,
                        path: selectedNote.path,
                        content: content
                    )
                }.value
                await MainActor.run {
                    propertiesStatus = "properties saved"
                    appErrorState = nil
                    isSavingProperties = false
                    loadSelectedNote(path: selectedNote.path)
                }
            } catch {
                await MainActor.run {
                    propertiesError = "properties save failed: \(error)"
                    presentAppError(
                        error,
                        operation: "Save properties",
                        recoveryAction: .retrySaveProperties
                    )
                    isSavingProperties = false
                }
            }
        }
    }

    private func loadLinkPanels(path: String) {
        let root = vaultRoot.trimmingCharacters(in: .whitespacesAndNewlines)
        let db = dbPath.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !root.isEmpty, !db.isEmpty else {
            linksError = "open a vault before loading links"
            return
        }

        isLoadingLinks = true
        linksError = nil

        Task {
            do {
                let panels = try await Task.detached(priority: .userInitiated) {
                    try ObsBridgeClient().noteLinks(vaultRoot: root, dbPath: db, path: path)
                }.value
                await MainActor.run {
                    linkPanels = panels
                    appErrorState = nil
                    isLoadingLinks = false
                }
            } catch {
                await MainActor.run {
                    linkPanels = nil
                    linksError = "links load failed: \(error)"
                    presentAppError(
                        error,
                        operation: "Load links",
                        recoveryAction: .retryLoadLinks(path: path)
                    )
                    isLoadingLinks = false
                }
            }
        }
    }

    private func loadBasesList() {
        let root = vaultRoot.trimmingCharacters(in: .whitespacesAndNewlines)
        let db = dbPath.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !root.isEmpty, !db.isEmpty else {
            basesError = "open a vault before loading bases"
            return
        }

        isLoadingBases = true
        basesError = nil

        Task {
            do {
                let refs = try await Task.detached(priority: .userInitiated) {
                    try ObsBridgeClient().basesList(vaultRoot: root, dbPath: db)
                }.value
                await MainActor.run {
                    baseRefs = refs
                    appErrorState = nil
                    isLoadingBases = false
                    basePageNumber = 1

                    guard let first = refs.first else {
                        selectedBaseId = nil
                        selectedBaseViewName = nil
                        baseTablePage = nil
                        return
                    }

                    let nextBaseId =
                        if let selectedBaseId, refs.contains(where: { $0.baseId == selectedBaseId }) {
                            selectedBaseId
                        } else {
                            first.baseId
                        }
                    let selectedBase = refs.first(where: { $0.baseId == nextBaseId }) ?? first
                    let nextViewName =
                        if let selectedBaseViewName,
                            selectedBase.views.contains(selectedBaseViewName)
                        {
                            selectedBaseViewName
                        } else {
                            selectedBase.views.first
                        }
                    let shouldReloadCurrent =
                        selectedBaseId == nextBaseId && selectedBaseViewName == nextViewName
                    selectedBaseId = nextBaseId
                    selectedBaseViewName = nextViewName

                    if shouldReloadCurrent {
                        loadSelectedBasePage()
                    }
                }
            } catch {
                await MainActor.run {
                    basesError = "bases load failed: \(error)"
                    presentAppError(
                        error,
                        operation: "Load bases",
                        recoveryAction: .retryLoadBasesList
                    )
                    isLoadingBases = false
                }
            }
        }
    }

    private func loadSelectedBasePage() {
        guard let selectedBaseId, let selectedBaseViewName else {
            baseTablePage = nil
            return
        }

        let root = vaultRoot.trimmingCharacters(in: .whitespacesAndNewlines)
        let db = dbPath.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !root.isEmpty, !db.isEmpty else {
            basesError = "open a vault before loading base table rows"
            return
        }

        let requestBaseId = selectedBaseId
        let requestViewName = selectedBaseViewName
        let requestPage = basePageNumber
        let requestPageSize = basePageSize

        isLoadingBasePage = true
        basesError = nil

        Task {
            do {
                let page = try await Task.detached(priority: .userInitiated) {
                    try ObsBridgeClient().basesView(
                        vaultRoot: root,
                        dbPath: db,
                        pathOrId: requestBaseId,
                        viewName: requestViewName,
                        page: requestPage,
                        pageSize: requestPageSize
                    )
                }.value
                await MainActor.run {
                    if selectedBaseId == requestBaseId
                        && selectedBaseViewName == requestViewName
                        && basePageNumber == requestPage
                    {
                        baseTablePage = page
                        appErrorState = nil
                    }
                    isLoadingBasePage = false
                }
            } catch {
                await MainActor.run {
                    basesError = "bases table load failed: \(error)"
                    presentAppError(
                        error,
                        operation: "Load base page",
                        recoveryAction: .retryLoadBasePage
                    )
                    baseTablePage = nil
                    isLoadingBasePage = false
                }
            }
        }
    }

    private func basePaginationSummary(for page: BridgeBaseTablePage) -> String {
        guard page.total > 0 else {
            return "Rows 0 of 0"
        }

        let start = UInt64(page.page - 1) * UInt64(page.pageSize) + 1
        let end = min(page.total, UInt64(page.page) * UInt64(page.pageSize))
        return "Rows \(start)-\(end) of \(page.total)"
    }

    private func baseRowSummary(_ row: BridgeBaseTableRow) -> String {
        let columns = visibleBaseColumns
        guard !columns.isEmpty else {
            return row.values
                .keys
                .sorted()
                .compactMap { key in
                    guard let value = row.values[key], !value.isEmpty else {
                        return nil
                    }
                    return "\(key): \(value)"
                }
                .joined(separator: " | ")
        }

        return columns
            .compactMap { column in
                guard let value = row.values[column.key], !value.isEmpty else {
                    return nil
                }
                return "\(column.key): \(value)"
            }
            .joined(separator: " | ")
    }

    private func presentQuickOpen() {
        guard openedVaultRoot != nil else {
            return
        }

        selectedSidebarItem = .notes
        if !fileTreeViewModel.hasLoadedNotes {
            fileTreeViewModel.loadNextPage()
        }
        quickOpenQuery = ""
        isQuickOpenPresented = true
    }

    private func openNoteFromQuickOpen(path: String) {
        if selectedTreePath == path {
            loadSelectedNote(path: path)
        } else {
            selectedTreePath = path
        }
        isQuickOpenPresented = false
    }
}
