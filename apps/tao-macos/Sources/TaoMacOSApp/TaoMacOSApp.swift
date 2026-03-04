import AppKit
import SwiftUI
import TaoMacOSAppScaffold

@main
struct TaoMacOSApp: App {
    @AppStorage("tao.settings.vault_root") private var vaultRoot = ""

    var body: some Scene {
        WindowGroup {
            TaoRootSplitView(vaultRoot: $vaultRoot)
                .frame(minWidth: 1100, minHeight: 720)
        }
        Settings {
            TaoSettingsView(vaultRoot: $vaultRoot)
                .frame(width: 620)
                .padding(20)
        }
    }
}

private struct TaoRootSplitView: View {
    @Binding var vaultRoot: String
    @Environment(\.openSettings) private var openSettings
    @StateObject private var fileTreeViewModel = FileTreeViewModel()
    @State private var selectedPath: String?
    @State private var selectedNoteContext: BridgeNoteContext?
    @State private var noteError: String?
    @State private var isLoadingNote = false
    @State private var showNoteLoading = false
    @State private var noteLoadTask: Task<Void, Never>?

    private var normalizedVaultRoot: String {
        vaultRoot.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private var sidebarSubtitle: String {
        if normalizedVaultRoot.isEmpty {
            return "No vault selected"
        }
        if let error = fileTreeViewModel.errorMessage {
            return error
        }
        if let stats = fileTreeViewModel.stats {
            return "\(stats.markdownFiles) notes indexed"
        }
        return "Loading notes"
    }

    var body: some View {
        NavigationSplitView {
            VStack(alignment: .leading, spacing: 12) {
                HStack {
                    Text("Files")
                        .font(.headline)
                    Spacer()
                    Button("Settings") {
                        openSettings()
                    }
                    .buttonStyle(.borderless)
                    .font(.caption)
                }

                Text(sidebarSubtitle)
                    .font(.caption)
                    .foregroundStyle(
                        fileTreeViewModel.errorMessage == nil
                            ? AnyShapeStyle(.secondary)
                            : AnyShapeStyle(.red)
                    )

                if normalizedVaultRoot.isEmpty {
                    ContentUnavailableView(
                        "Select a Vault",
                        systemImage: "folder",
                        description: Text("Open Settings and choose your vault folder.")
                    )
                } else if let error = fileTreeViewModel.errorMessage, fileTreeViewModel.roots.isEmpty {
                    VStack(alignment: .leading, spacing: 8) {
                        ContentUnavailableView(
                            "Load Failed",
                            systemImage: "exclamationmark.triangle",
                            description: Text(error)
                        )
                        Button("Retry") {
                            fileTreeViewModel.reload()
                        }
                        .buttonStyle(.bordered)
                    }
                } else {
                    List(selection: $selectedPath) {
                        OutlineGroup(fileTreeViewModel.roots, children: \.children) { node in
                            Label(node.name, systemImage: node.isFile ? "doc.text" : "folder")
                                .tag(Optional(node.path))
                        }

                        if fileTreeViewModel.isLoading && fileTreeViewModel.roots.isEmpty {
                            HStack {
                                Spacer()
                                ProgressView()
                                Spacer()
                            }
                            .listRowSeparator(.hidden)
                        }
                    }
                    .listStyle(.sidebar)
                }
            }
            .padding(.horizontal, 10)
            .padding(.top, 12)
            .navigationSplitViewColumnWidth(min: 260, ideal: 320)
        } detail: {
            NoteDetailPane(
                noteContext: selectedNoteContext,
                noteError: noteError,
                isLoadingNote: isLoadingNote,
                showNoteLoading: showNoteLoading,
                vaultRoot: normalizedVaultRoot
            )
        }
        .onAppear {
            bindVaultIfNeeded()
        }
        .onChange(of: vaultRoot) { _, _ in
            bindVaultIfNeeded()
        }
        .onChange(of: selectedPath) { _, newValue in
            guard let path = newValue, fileTreeViewModel.isNotePath(path) else {
                return
            }
            loadNote(path: path)
        }
    }

    private func bindVaultIfNeeded() {
        noteLoadTask?.cancel()
        selectedPath = nil
        selectedNoteContext = nil
        noteError = nil
        showNoteLoading = false
        isLoadingNote = false
        fileTreeViewModel.bindVault(vaultRoot: normalizedVaultRoot, eagerLoad: !normalizedVaultRoot.isEmpty)
    }

    private func loadNote(path: String) {
        guard !normalizedVaultRoot.isEmpty else {
            return
        }

        noteLoadTask?.cancel()
        isLoadingNote = true
        showNoteLoading = false
        noteError = nil

        let requestVault = normalizedVaultRoot
        let requestPath = path

        noteLoadTask = Task {
            let spinnerTask = Task {
                try? await Task.sleep(for: .milliseconds(120))
                guard !Task.isCancelled else {
                    return
                }
                await MainActor.run {
                    if isLoadingNote {
                        showNoteLoading = true
                    }
                }
            }

            do {
                let context = try await Task.detached(priority: .userInitiated) {
                    try TaoBridgeClient().noteContext(
                        vaultRoot: requestVault,
                        dbPath: "",
                        path: requestPath
                    )
                }.value

                spinnerTask.cancel()
                await MainActor.run {
                    selectedNoteContext = context
                    isLoadingNote = false
                    showNoteLoading = false
                }
            } catch {
                spinnerTask.cancel()
                await MainActor.run {
                    noteError = "Unable to open note: \(error)"
                    isLoadingNote = false
                    showNoteLoading = false
                }
            }
        }
    }
}

private struct NoteDetailPane: View {
    let noteContext: BridgeNoteContext?
    let noteError: String?
    let isLoadingNote: Bool
    let showNoteLoading: Bool
    let vaultRoot: String

    private var properties: [(key: String, value: String)] {
        guard let noteContext else {
            return []
        }
        return parseFrontMatter(noteContext.note.frontMatter)
    }

    var body: some View {
        if vaultRoot.isEmpty {
            ContentUnavailableView(
                "No Vault Open",
                systemImage: "square.stack.3d.down.right",
                description: Text("Choose a vault in Settings to begin.")
            )
        } else if let noteError {
            ContentUnavailableView("Read Failed", systemImage: "exclamationmark.triangle", description: Text(noteError))
        } else if showNoteLoading && isLoadingNote {
            VStack {
                ProgressView("Opening note...")
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity)
        } else if let noteContext {
            ScrollView {
                VStack(alignment: .leading, spacing: 18) {
                    Text(noteContext.note.title)
                        .font(.title2.weight(.semibold))

                    if !properties.isEmpty {
                        VStack(alignment: .leading, spacing: 8) {
                            Text("Properties")
                                .font(.headline)
                            ForEach(properties, id: \.key) { entry in
                                HStack(alignment: .firstTextBaseline, spacing: 12) {
                                    Text(entry.key)
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(.secondary)
                                        .frame(width: 160, alignment: .leading)
                                    Text(entry.value)
                                        .font(.callout.monospaced())
                                        .textSelection(.enabled)
                                    Spacer(minLength: 0)
                                }
                            }
                        }
                        .padding(12)
                        .background(.quaternary.opacity(0.4), in: RoundedRectangle(cornerRadius: 8))
                    }

                    Text(.init(noteContext.note.body))
                        .font(.body)
                        .textSelection(.enabled)
                        .frame(maxWidth: .infinity, alignment: .leading)
                }
                .frame(maxWidth: .infinity, alignment: .topLeading)
                .padding(24)
            }
        } else {
            ContentUnavailableView("Select a Note", systemImage: "doc.text", description: Text("Choose a markdown file from the sidebar."))
        }
    }
}

private struct TaoSettingsView: View {
    @Binding var vaultRoot: String
    @State private var draftVaultRoot: String

    init(vaultRoot: Binding<String>) {
        _vaultRoot = vaultRoot
        _draftVaultRoot = State(initialValue: vaultRoot.wrappedValue)
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text("Vault Settings")
                .font(.title3.weight(.semibold))

            Text("Choose the vault folder to load in Tao.")
                .font(.callout)
                .foregroundStyle(.secondary)

            TextField("/absolute/path/to/vault", text: $draftVaultRoot)
                .textFieldStyle(.roundedBorder)

            HStack(spacing: 10) {
                Button("Choose Folder") {
                    chooseVaultFolder()
                }
                Button("Save") {
                    vaultRoot = draftVaultRoot.trimmingCharacters(in: .whitespacesAndNewlines)
                }
                .keyboardShortcut(.defaultAction)
            }
        }
        .onChange(of: vaultRoot) { _, newValue in
            draftVaultRoot = newValue
        }
    }

    private func chooseVaultFolder() {
        let panel = NSOpenPanel()
        panel.prompt = "Select"
        panel.canChooseFiles = false
        panel.canChooseDirectories = true
        panel.canCreateDirectories = true
        panel.allowsMultipleSelection = false
        if panel.runModal() == .OK, let url = panel.url {
            draftVaultRoot = url.path
        }
    }
}

private func parseFrontMatter(_ raw: String?) -> [(key: String, value: String)] {
    guard let raw else {
        return []
    }
    return raw
        .split(separator: "\n")
        .compactMap { line in
            let parts = line.split(separator: ":", maxSplits: 1, omittingEmptySubsequences: false)
            guard parts.count == 2 else {
                return nil
            }
            return (
                key: parts[0].trimmingCharacters(in: .whitespacesAndNewlines),
                value: parts[1].trimmingCharacters(in: .whitespacesAndNewlines)
            )
        }
}
