import SwiftUI
import AppKit
import ObsMacOSAppScaffold

@main
struct ObsMacOSApp: App {
    var body: some Scene {
        WindowGroup {
            ObsRootSplitView()
                .frame(minWidth: 1120, minHeight: 720)
        }
    }
}

private enum SidebarItem: String, CaseIterable, Identifiable {
    case vault = "Vault"
    case notes = "Notes"
    case bases = "Bases"

    var id: String { rawValue }
}

private struct ObsRootSplitView: View {
    @State private var selectedSidebarItem: SidebarItem? = .notes
    @State private var selectedTreePath: String?
    @State private var vaultRoot = ""
    @State private var dbPath = ""
    @State private var openedVaultRoot: String?
    @State private var statsSummary = "Bridge read APIs not called yet."
    @State private var bridgeError: String?
    @State private var isLoadingStats = false
    @StateObject private var fileTreeViewModel = FileTreeViewModel()

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
                if let selectedTreePath {
                    Text("Selected: \(selectedTreePath)")
                        .foregroundStyle(.secondary)
                } else {
                    Text("Select a file from the tree to inspect its path.")
                        .foregroundStyle(.secondary)
                }
                Spacer()
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            .padding(20)
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
            return "Bases table pane placeholder"
        case .none:
            return "Select a section"
        }
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
            Text("Bases screen scaffold")
                .font(.callout)
                .foregroundStyle(.secondary)
                .frame(maxWidth: .infinity, alignment: .leading)
        case .none:
            EmptyView()
        }
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
                    isLoadingStats = false
                }
            } catch {
                await MainActor.run {
                    bridgeError = "bridge read failed: \(error)"
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
}
