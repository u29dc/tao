import Foundation
import TaoMacOSAppScaffold

struct FileTreeNode: Identifiable, Hashable {
    let id: String
    let name: String
    let path: String
    let isFile: Bool
    var children: [FileTreeNode]?
}

@MainActor
final class FileTreeViewModel: ObservableObject {
    @Published private(set) var roots: [FileTreeNode] = []
    @Published private(set) var canLoadMore = false
    @Published private(set) var isLoading = false
    @Published var errorMessage: String?

    private var vaultRoot = ""
    private var dbPath = ""
    private var nextCursor: String?
    private var summaries: [BridgeNoteSummary] = []

    var hasLoadedNotes: Bool {
        !summaries.isEmpty
    }

    func bindVault(vaultRoot: String, dbPath: String, eagerLoad: Bool = true) {
        guard self.vaultRoot != vaultRoot || self.dbPath != dbPath else {
            return
        }

        self.vaultRoot = vaultRoot
        self.dbPath = dbPath
        self.nextCursor = nil
        self.summaries = []
        self.roots = []
        self.canLoadMore = false
        self.errorMessage = nil
        if eagerLoad {
            loadNextPage()
        }
    }

    func loadNextPage() {
        guard !isLoading else {
            return
        }
        guard !vaultRoot.isEmpty, !dbPath.isEmpty else {
            errorMessage = "open a vault first"
            return
        }

        isLoading = true
        errorMessage = nil

        let requestVaultRoot = vaultRoot
        let requestDbPath = dbPath
        let requestCursor = nextCursor

        Task {
            do {
                let page = try await Task.detached(priority: .userInitiated) {
                    try TaoBridgeClient().notesList(
                        vaultRoot: requestVaultRoot,
                        dbPath: requestDbPath,
                        afterPath: requestCursor,
                        limit: 1000
                    )
                }.value

                await MainActor.run {
                    summaries.append(contentsOf: page.items)
                    nextCursor = page.nextCursor
                    canLoadMore = page.nextCursor != nil
                    roots = FileTreeBuilder.build(from: summaries)
                    isLoading = false
                }
            } catch {
                await MainActor.run {
                    errorMessage = "file tree load failed: \(error)"
                    isLoading = false
                }
            }
        }
    }

    func quickOpenMatches(query: String, limit: Int = 25) -> [BridgeNoteSummary] {
        let trimmed = query.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.isEmpty {
            return Array(summaries.prefix(limit))
        }

        let needle = trimmed.lowercased()
        return summaries
            .filter { summary in
                summary.path.lowercased().contains(needle)
                    || summary.title.lowercased().contains(needle)
            }
            .prefix(limit)
            .map { $0 }
    }
}

private enum FileTreeBuilder {
    static func build(from summaries: [BridgeNoteSummary]) -> [FileTreeNode] {
        let root = MutableNode(name: "", path: "", isFile: false)
        for summary in summaries {
            insert(summary.path, into: root)
        }
        return root.sortedChildren()
    }

    private static func insert(_ path: String, into root: MutableNode) {
        let components = path.split(separator: "/").map(String.init)
        guard !components.isEmpty else {
            return
        }

        var current = root
        var runningPath = ""
        for (index, component) in components.enumerated() {
            runningPath = runningPath.isEmpty ? component : "\(runningPath)/\(component)"
            let isLast = index == components.count - 1
            current = current.child(named: component, path: runningPath, isFile: isLast)
        }
    }
}

private final class MutableNode {
    private let name: String
    private let path: String
    private let isFile: Bool
    private var childrenByName: [String: MutableNode] = [:]

    init(name: String, path: String, isFile: Bool) {
        self.name = name
        self.path = path
        self.isFile = isFile
    }

    func child(named name: String, path: String, isFile: Bool) -> MutableNode {
        if let existing = childrenByName[name] {
            return existing
        }

        let node = MutableNode(name: name, path: path, isFile: isFile)
        childrenByName[name] = node
        return node
    }

    func sortedChildren() -> [FileTreeNode] {
        childrenByName
            .values
            .sorted { left, right in
                if left.isFile != right.isFile {
                    return !left.isFile
                }
                return left.name.localizedCaseInsensitiveCompare(right.name) == .orderedAscending
            }
            .map { child in
                let descendants = child.sortedChildren()
                return FileTreeNode(
                    id: child.path,
                    name: child.name,
                    path: child.path,
                    isFile: child.isFile,
                    children: descendants.isEmpty ? nil : descendants
                )
            }
    }
}
