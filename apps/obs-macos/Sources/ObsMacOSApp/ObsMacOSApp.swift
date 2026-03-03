import SwiftUI
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
                Text("Split-layout scaffolding for upcoming vault/navigation/note panes.")
                    .foregroundStyle(.secondary)
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
            return "Vault overview pane placeholder"
        case .notes:
            return "Note list and reader pane placeholder"
        case .bases:
            return "Bases table pane placeholder"
        case .none:
            return "Select a section"
        }
    }
}
