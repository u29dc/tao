use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum QueryDocsColumn {
    FileId,
    Path,
    Title,
    MatchedIn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GraphNeighborDirection {
    All,
    Outgoing,
    Incoming,
}

impl GraphNeighborDirection {
    pub(crate) fn parse(raw: &str) -> Result<Self> {
        match raw {
            "all" => Ok(Self::All),
            "outgoing" => Ok(Self::Outgoing),
            "incoming" => Ok(Self::Incoming),
            _ => Err(anyhow!(
                "unsupported --direction '{}'; expected one of: all|outgoing|incoming",
                raw
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GraphComponentModeArg {
    Weak,
    Strong,
}

impl GraphComponentModeArg {
    pub(crate) fn parse(raw: &str) -> Result<Self> {
        match raw {
            "weak" => Ok(Self::Weak),
            "strong" => Ok(Self::Strong),
            _ => Err(anyhow!(
                "unsupported --mode '{}'; expected one of: weak|strong",
                raw
            )),
        }
    }

    pub(crate) fn as_service_mode(self) -> tao_sdk_service::GraphComponentMode {
        match self {
            Self::Weak => tao_sdk_service::GraphComponentMode::Weak,
            Self::Strong => tao_sdk_service::GraphComponentMode::Strong,
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Weak => "weak",
            Self::Strong => "strong",
        }
    }
}

impl QueryDocsColumn {
    pub(crate) fn key(self) -> &'static str {
        match self {
            Self::FileId => "file_id",
            Self::Path => "path",
            Self::Title => "title",
            Self::MatchedIn => "matched_in",
        }
    }
}

pub(crate) fn parse_query_docs_columns(select: Option<&str>) -> Result<Vec<QueryDocsColumn>> {
    let mut columns = Vec::new();
    let mut seen = HashSet::new();

    if let Some(raw) = select {
        let tokens = raw
            .split(',')
            .map(str::trim)
            .filter(|token| !token.is_empty())
            .collect::<Vec<_>>();
        if tokens.is_empty() {
            return Err(anyhow!(
                "--select must include at least one docs column from: file_id,path,title,matched_in"
            ));
        }
        for token in tokens {
            let column = match token {
                "file_id" => QueryDocsColumn::FileId,
                "path" => QueryDocsColumn::Path,
                "title" => QueryDocsColumn::Title,
                "matched_in" => QueryDocsColumn::MatchedIn,
                _ => {
                    return Err(anyhow!(
                        "unsupported docs projection column '{}'; allowed: file_id,path,title,matched_in",
                        token
                    ));
                }
            };
            if seen.insert(column) {
                columns.push(column);
            }
        }
    } else {
        columns = vec![
            QueryDocsColumn::FileId,
            QueryDocsColumn::Path,
            QueryDocsColumn::Title,
            QueryDocsColumn::MatchedIn,
        ];
    }

    Ok(columns)
}

pub(crate) fn project_query_docs_row(
    item: SearchQueryProjectedItem,
    columns: &[QueryDocsColumn],
) -> JsonValue {
    let row = query_docs_row(item);
    JsonValue::Object(project_query_docs_row_map(&row, columns))
}

pub(crate) fn query_docs_row(item: SearchQueryProjectedItem) -> serde_json::Map<String, JsonValue> {
    let mut map = serde_json::Map::with_capacity(5);
    map.insert(
        QueryDocsColumn::FileId.key().to_string(),
        JsonValue::String(item.file_id.unwrap_or_default()),
    );
    map.insert(
        QueryDocsColumn::Path.key().to_string(),
        JsonValue::String(item.path.unwrap_or_default()),
    );
    map.insert(
        QueryDocsColumn::Title.key().to_string(),
        JsonValue::String(item.title.unwrap_or_default()),
    );
    map.insert("indexed_at".to_string(), JsonValue::String(item.indexed_at));
    map.insert(
        QueryDocsColumn::MatchedIn.key().to_string(),
        JsonValue::Array(
            item.matched_in
                .unwrap_or_default()
                .into_iter()
                .map(JsonValue::String)
                .collect::<Vec<_>>(),
        ),
    );
    map
}

pub(crate) fn project_query_docs_row_map(
    row: &serde_json::Map<String, JsonValue>,
    columns: &[QueryDocsColumn],
) -> serde_json::Map<String, JsonValue> {
    let mut map = serde_json::Map::with_capacity(columns.len());
    for column in columns {
        let key = column.key();
        if let Some(value) = row.get(key) {
            map.insert(key.to_string(), value.clone());
        }
    }
    map
}

pub(crate) struct QueryDocsStreamingEnvelope<'a> {
    pub(crate) page: &'a tao_sdk_search::SearchQueryProjectedPage,
    pub(crate) columns: &'a [QueryDocsColumn],
    pub(crate) elapsed: u128,
}

pub(crate) struct QueryDocsStreamingData<'a> {
    page: &'a tao_sdk_search::SearchQueryProjectedPage,
    columns: &'a [QueryDocsColumn],
}

pub(crate) struct QueryDocsStreamingMeta<'a> {
    page: &'a tao_sdk_search::SearchQueryProjectedPage,
    elapsed: u128,
}

pub(crate) struct QueryDocsStreamingRows<'a> {
    items: &'a [SearchQueryProjectedItem],
    columns: &'a [QueryDocsColumn],
}

pub(crate) struct QueryDocsStreamingRow<'a> {
    item: &'a SearchQueryProjectedItem,
    columns: &'a [QueryDocsColumn],
}

pub(crate) fn query_docs_projection(columns: &[QueryDocsColumn]) -> SearchQueryProjection {
    SearchQueryProjection {
        include_file_id: columns.contains(&QueryDocsColumn::FileId),
        include_path: columns.contains(&QueryDocsColumn::Path),
        include_title: columns.contains(&QueryDocsColumn::Title),
        include_matched_in: columns.contains(&QueryDocsColumn::MatchedIn),
    }
}

#[derive(Debug)]
pub(crate) struct QueryPostFilterAccumulator {
    offset: usize,
    limit: usize,
    sort_keys: Vec<SortKey>,
    total: u64,
    rows: Vec<serde_json::Map<String, JsonValue>>,
}

impl QueryPostFilterAccumulator {
    pub(crate) fn new(offset: u32, limit: u32, sort_keys: &[SortKey]) -> Self {
        Self {
            offset: offset as usize,
            limit: limit as usize,
            sort_keys: sort_keys.to_vec(),
            total: 0,
            rows: Vec::new(),
        }
    }

    pub(crate) fn push_batch(&mut self, batch: Vec<serde_json::Map<String, JsonValue>>) {
        if self.sort_keys.is_empty() {
            for row in batch {
                let row_index = usize::try_from(self.total).unwrap_or(usize::MAX);
                self.total = self.total.saturating_add(1);
                if row_index < self.offset {
                    continue;
                }
                if self.rows.len() < self.limit {
                    self.rows.push(row);
                }
            }
            return;
        }

        let window_size = self.offset.saturating_add(self.limit);
        self.total = self
            .total
            .saturating_add(u64::try_from(batch.len()).unwrap_or(u64::MAX));
        if window_size == 0 {
            return;
        }
        self.rows.extend(batch);
        apply_sort(&mut self.rows, &self.sort_keys);
        if self.rows.len() > window_size {
            self.rows.truncate(window_size);
        }
    }

    pub(crate) fn finish(mut self) -> (u64, Vec<JsonValue>) {
        let rows = if self.sort_keys.is_empty() {
            self.rows
        } else {
            self.rows = self
                .rows
                .into_iter()
                .skip(self.offset)
                .take(self.limit)
                .collect::<Vec<_>>();
            self.rows
        };
        (
            self.total,
            rows.into_iter().map(JsonValue::Object).collect::<Vec<_>>(),
        )
    }

    pub(crate) fn finish_query_docs(self, columns: &[QueryDocsColumn]) -> (u64, Vec<JsonValue>) {
        let (total, rows) = self.finish();
        (
            total,
            rows.into_iter()
                .filter_map(|row| match row {
                    JsonValue::Object(row) => {
                        Some(JsonValue::Object(project_query_docs_row_map(&row, columns)))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )
    }
}

pub(crate) fn apply_post_filter_batch(
    batch: Vec<serde_json::Map<String, JsonValue>>,
    where_expr: Option<&WhereExpr>,
) -> Result<Vec<serde_json::Map<String, JsonValue>>> {
    apply_where_filter(batch, where_expr)
        .map_err(|source| anyhow!("evaluate --where failed: {source}"))
}

pub(crate) fn flatten_base_query_row(
    row: serde_json::Map<String, JsonValue>,
) -> serde_json::Map<String, JsonValue> {
    let mut flattened = serde_json::Map::<String, JsonValue>::new();
    if let Some(file_id) = row.get("file_id") {
        flattened.insert("file_id".to_string(), file_id.clone());
    }
    if let Some(file_path) = row.get("file_path") {
        flattened.insert("path".to_string(), file_path.clone());
    }
    if let Some(values) = row.get("values").and_then(JsonValue::as_object) {
        for (key, value) in values {
            flattened.insert(key.clone(), value.clone());
        }
    }
    flattened
}

pub(crate) fn collect_docs_rows_for_where_only(
    runtime: &mut RuntimeMode,
    resolved: &ResolvedVaultPathArgs,
    query: &str,
    columns: &[QueryDocsColumn],
    where_expr: &WhereExpr,
    limit: u32,
    offset: u32,
) -> Result<(u64, Vec<JsonValue>)> {
    with_connection(runtime, resolved, |connection| {
        let mut query_offset = 0_u64;
        let mut total = 0_u64;
        let mut rows = Vec::new();

        loop {
            let page = SearchQueryService.query_projected(
                Path::new(&resolved.vault_root),
                connection,
                SearchQueryRequest {
                    query: query.to_string(),
                    limit: QUERY_DOCS_POST_FILTER_PAGE_LIMIT,
                    offset: query_offset,
                },
                SearchQueryProjection::default(),
            )?;
            let batch_count = u64::try_from(page.items.len()).unwrap_or(u64::MAX);
            if batch_count == 0 {
                break;
            }

            let batch_rows = page
                .items
                .into_iter()
                .map(query_docs_row)
                .collect::<Vec<_>>();
            let filtered = apply_where_filter(batch_rows, Some(where_expr))
                .map_err(|source| anyhow!("evaluate --where failed: {source}"))?;
            for row in filtered {
                if total >= u64::from(offset) && rows.len() < limit as usize {
                    rows.push(JsonValue::Object(project_query_docs_row_map(&row, columns)));
                }
                total = total.saturating_add(1);
            }

            query_offset = query_offset.saturating_add(batch_count);
            if query_offset >= page.total {
                break;
            }
        }

        Ok((total, rows))
    })
}

impl Serialize for QueryDocsStreamingEnvelope<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("ok", &true)?;
        map.serialize_entry(
            "data",
            &QueryDocsStreamingData {
                page: self.page,
                columns: self.columns,
            },
        )?;
        map.serialize_entry(
            "meta",
            &QueryDocsStreamingMeta {
                page: self.page,
                elapsed: self.elapsed,
            },
        )?;
        map.end()
    }
}

impl Serialize for QueryDocsStreamingData<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(6))?;
        let column_names = self
            .columns
            .iter()
            .map(|column| column.key())
            .collect::<Vec<_>>();
        map.serialize_entry("from", "docs")?;
        map.serialize_entry("columns", &column_names)?;
        map.serialize_entry(
            "rows",
            &QueryDocsStreamingRows {
                items: &self.page.items,
                columns: self.columns,
            },
        )?;
        map.serialize_entry("total", &self.page.total)?;
        map.serialize_entry("limit", &self.page.limit)?;
        map.serialize_entry("offset", &self.page.offset)?;
        map.end()
    }
}

impl Serialize for QueryDocsStreamingMeta<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let count = u64::try_from(self.page.items.len()).unwrap_or(u64::MAX);
        let has_more = self.page.offset.saturating_add(count) < self.page.total;
        let mut map = serializer.serialize_map(Some(5))?;
        map.serialize_entry("tool", "query.run")?;
        map.serialize_entry("elapsed", &self.elapsed)?;
        map.serialize_entry("count", &count)?;
        map.serialize_entry("total", &self.page.total)?;
        map.serialize_entry("hasMore", &has_more)?;
        map.end()
    }
}

impl Serialize for QueryDocsStreamingRows<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(self.items.len()))?;
        for item in self.items {
            sequence.serialize_element(&QueryDocsStreamingRow {
                item,
                columns: self.columns,
            })?;
        }
        sequence.end()
    }
}

impl Serialize for QueryDocsStreamingRow<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.columns.len()))?;
        for column in self.columns {
            match column {
                QueryDocsColumn::FileId => {
                    map.serialize_entry(
                        column.key(),
                        &self.item.file_id.clone().unwrap_or_default(),
                    )?;
                }
                QueryDocsColumn::Path => {
                    map.serialize_entry(column.key(), &self.item.path.clone().unwrap_or_default())?;
                }
                QueryDocsColumn::Title => {
                    map.serialize_entry(
                        column.key(),
                        &self.item.title.clone().unwrap_or_default(),
                    )?;
                }
                QueryDocsColumn::MatchedIn => {
                    map.serialize_entry(
                        column.key(),
                        &self.item.matched_in.clone().unwrap_or_default(),
                    )?;
                }
            }
        }
        map.end()
    }
}
