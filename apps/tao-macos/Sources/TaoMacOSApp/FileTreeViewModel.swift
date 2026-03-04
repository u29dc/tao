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
    @Published private(set) var isLoading = false
    @Published private(set) var stats: BridgeVaultStats?
    @Published var errorMessage: String?

    private var vaultRoot = ""
    private var summaries: [BridgeNoteSummary] = []
    private var notePaths: Set<String> = []
    private var loadTask: Task<Void, Never>?

    var hasLoadedNotes: Bool {
        !summaries.isEmpty
    }

    func bindVault(vaultRoot: String, eagerLoad: Bool = true) {
        let normalizedVaultRoot = vaultRoot.trimmingCharacters(in: .whitespacesAndNewlines)
        guard self.vaultRoot != normalizedVaultRoot else {
            if eagerLoad {
                reload()
            }
            return
        }

        loadTask?.cancel()
        self.vaultRoot = normalizedVaultRoot
        self.summaries = []
        self.notePaths = []
        self.roots = []
        self.stats = nil
        self.errorMessage = nil
        if eagerLoad {
            reload()
        }
    }

    func reload() {
        loadTask?.cancel()
        guard !vaultRoot.isEmpty else {
            errorMessage = "open a vault first"
            return
        }

        isLoading = true
        errorMessage = nil

        let requestVaultRoot = vaultRoot

        loadTask = Task {
            do {
                let (stats, allNotes) = try await Task.detached(priority: .userInitiated) {
                    let client = TaoBridgeClient()
                    let startup = try client.startupBundle(
                        vaultRoot: requestVaultRoot,
                        dbPath: "",
                        limit: 1_000
                    )
                    var notes = startup.notes.items
                    var cursor = startup.notes.nextCursor
                    var seenCursors: Set<String> = []
                    while let cursorPath = cursor {
                        if !seenCursors.insert(cursorPath).inserted {
                            throw FileTreeLoadError.cursorCycle(cursorPath)
                        }
                        let page = try client.notesList(
                            vaultRoot: requestVaultRoot,
                            dbPath: "",
                            afterPath: cursorPath,
                            limit: 1_000
                        )
                        notes.append(contentsOf: page.items)
                        cursor = page.nextCursor
                    }
                    return (startup.stats, notes)
                }.value

                await MainActor.run {
                    summaries = allNotes
                        .sorted { left, right in
                            left.path.localizedCaseInsensitiveCompare(right.path) == .orderedAscending
                        }
                    notePaths = Set(summaries.map(\.path))
                    roots = FileTreeBuilder.build(from: summaries)
                    self.stats = stats
                    isLoading = false
                    loadTask = nil
                }
            } catch {
                await MainActor.run {
                    errorMessage = "file tree load failed: \(error)"
                    isLoading = false
                    loadTask = nil
                }
            }
        }
    }

    func isNotePath(_ path: String) -> Bool {
        notePaths.contains(path)
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

private enum FileTreeLoadError: LocalizedError {
    case cursorCycle(String)

    var errorDescription: String? {
        switch self {
        case .cursorCycle(let cursor):
            return "notes pagination cursor did not advance (\(cursor))"
        }
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
