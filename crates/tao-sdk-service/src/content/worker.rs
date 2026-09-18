//! Local backends use direct arguments and a fixed Linux limiter, with enforced deadlines and quotas.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::{Value, json};
use tao_sdk_storage::{ContentSegmentRecord, ExtractionJobRecord};

use super::{ContentError, MAX_SEGMENT_BYTES, directory_bytes};

const MAX_SCRATCH_BYTES: u64 = 96 * 1024 * 1024;
const MAX_PAGES: u32 = 1000;
const MAX_COMMAND_OUTPUT: u64 = 4 * 1024 * 1024;
const OCR_LANGUAGE: &str = "eng";

/// Explicit local runtime dependencies. No downloads, cloud OCR or implicit installation.
#[derive(Debug, Clone, Serialize)]
pub struct ContentCapabilities {
    pub backend: String,
    pub pdfinfo: Option<PathBuf>,
    pub pdftotext: Option<PathBuf>,
    pub pdftoppm: Option<PathBuf>,
    pub pdfimages: Option<PathBuf>,
    pub tesseract: Option<PathBuf>,
    pub ocr_languages: Vec<String>,
    pub ocr_model: Option<PathBuf>,
    pub ocr_model_revision: Option<String>,
    pub poppler_data: Option<PathBuf>,
    pub native_available: bool,
    pub ocr_available: bool,
    pub input_limit_bytes: u64,
    pub max_pages: u32,
    pub rendered_page_max_side: u32,
    pub worker_memory_limit_enforced: bool,
    pub worker_memory_watchdog: bool,
    pub worker_memory_limit_bytes: u64,
    pub max_parallel_workers: usize,
    pub diagnostics: Vec<String>,
}

pub fn content_capabilities() -> ContentCapabilities {
    let pdfinfo = find_program("pdfinfo");
    let pdftotext = find_program("pdftotext");
    let pdftoppm = find_program("pdftoppm");
    let pdfimages = find_program("pdfimages");
    let tesseract = find_program("tesseract");
    let native_available = pdfinfo.is_some() && pdftotext.is_some() && pdfimages.is_some();
    let model = tesseract.as_ref().and_then(|program| find_model(program));
    let model_revision = model.as_ref().and_then(|path| model_revision(path));
    let poppler_data = pdfinfo
        .as_ref()
        .and_then(|path| path.parent())
        .and_then(|path| path.parent())
        .map(|path| path.join("share/poppler"))
        .filter(|path| path.is_dir())
        .or_else(|| {
            ["/usr/share/poppler", "/usr/local/share/poppler"]
                .into_iter()
                .map(PathBuf::from)
                .find(|path| path.is_dir())
        });
    let ocr_available = tesseract.is_some() && pdftoppm.is_some() && model_revision.is_some();
    let mut diagnostics = Vec::new();
    if !native_available {
        diagnostics.push("Install/provision local Poppler tools pdfinfo, pdftotext and pdfimages on PATH; no automatic install is performed".to_string());
    }
    if !ocr_available {
        diagnostics.push("For local scanned-page OCR provision pdftoppm and Tesseract with the eng model on PATH".to_string());
    }
    diagnostics.push(if cfg!(target_os="linux") {"Worker address space is limited to 1 GiB and RSS is monitored at 512 MiB"} else {"This OS does not provide the qualified address-space limiter; workers are killed by a 512 MiB RSS watchdog, with sampling overshoot possible"}.to_string());
    ContentCapabilities {
        backend: "poppler-cli+tesseract-cli-v1".to_string(),
        pdfinfo,
        pdftotext,
        pdftoppm,
        pdfimages,
        tesseract,
        ocr_languages: vec![OCR_LANGUAGE.to_string()],
        ocr_model: model,
        ocr_model_revision: model_revision,
        poppler_data,
        native_available,
        ocr_available,
        input_limit_bytes: super::MAX_PDF_BYTES,
        max_pages: MAX_PAGES,
        rendered_page_max_side: 2048,
        worker_memory_limit_enforced: cfg!(target_os = "linux"),
        worker_memory_watchdog: cfg!(unix),
        worker_memory_limit_bytes: 512 * 1024 * 1024,
        max_parallel_workers: super::pool::worker_count(),
        diagnostics,
    }
}

pub(super) fn extractor_identity() -> String {
    let capabilities = content_capabilities();
    let programs=[&capabilities.pdfinfo,&capabilities.pdftotext,&capabilities.pdfimages,&capabilities.pdftoppm,&capabilities.tesseract].into_iter().map(|path|path.as_ref().map(|path|{let metadata=fs::metadata(path).ok();json!({"path":path,"size":metadata.as_ref().map(std::fs::Metadata::len),"modified":metadata.and_then(|value|value.modified().ok()).and_then(|value|value.duration_since(std::time::UNIX_EPOCH).ok()).map(|value|value.as_nanos().to_string())})})).collect::<Vec<_>>();
    let identity = json!({"adapter":capabilities.backend,"programs":programs,"language":OCR_LANGUAGE,"model_revision":capabilities.ocr_model_revision,"model_path":capabilities.ocr_model,"poppler_data":capabilities.poppler_data,"render_side":2048,"max_pages":MAX_PAGES,"ocr_min_mean_confidence":70});
    format!(
        "pdf-v1:{}",
        blake3::hash(identity.to_string().as_bytes()).to_hex()
    )
}

fn find_program(name: &str) -> Option<PathBuf> {
    std::env::var_os("PATH")
        .and_then(|paths| {
            std::env::split_paths(&paths)
                .map(|path| path.join(name))
                .find(|path| path.is_file())
        })
        .and_then(|path| path.canonicalize().ok())
}

pub(super) struct ExtractedPdf {
    pub segments: Vec<ContentSegmentRecord>,
    pub coverage: String,
    pub metadata: Value,
    pub diagnostics: Vec<String>,
}

pub(super) struct PdfWorkRequest<'a> {
    pub job: &'a ExtractionJobRecord,
    pub spool_root: &'a Path,
    pub scratch: &'a Path,
    pub deadline: Instant,
    pub cancelled: &'a AtomicBool,
    pub staged: &'a [ContentSegmentRecord],
    pub on_page: &'a mut dyn FnMut(&ContentSegmentRecord) -> Result<(), ContentError>,
}
pub(super) fn extract_pdf(request: PdfWorkRequest<'_>) -> Result<ExtractedPdf, ContentError> {
    let PdfWorkRequest {
        job,
        spool_root,
        scratch,
        deadline,
        cancelled,
        staged,
        on_page,
    } = request;
    if Path::new(&job.spool_name).components().count() != 1 || !job.spool_name.ends_with(".pdf") {
        return Err(ContentError::Invalid(
            "invalid captured PDF name".to_string(),
        ));
    }
    let input = spool_root.join(&job.spool_name);
    if fs::symlink_metadata(&input)?.file_type().is_symlink() {
        return Err(ContentError::Invalid(
            "captured PDF cannot be a symlink".to_string(),
        ));
    }
    if super::hash_bounded(&input, super::MAX_PDF_BYTES)? != job.desired_revision {
        return Err(ContentError::Invalid(
            "captured PDF revision failed integrity verification".to_string(),
        ));
    }
    let capabilities = content_capabilities();
    let pdfinfo = capabilities.pdfinfo.as_ref().ok_or_else(|| {
        ContentError::Invalid(
            "PDF native backend unavailable: provision local Poppler pdfinfo".to_string(),
        )
    })?;
    let pdftotext = capabilities.pdftotext.as_ref().ok_or_else(|| {
        ContentError::Invalid(
            "PDF native backend unavailable: provision local Poppler pdftotext".to_string(),
        )
    })?;
    let info = run(
        pdfinfo,
        &[input.as_os_str().to_os_string()],
        scratch,
        "info",
        deadline,
        cancelled,
    )?;
    let fields = info
        .lines()
        .filter_map(|line| line.split_once(':'))
        .map(|(key, value)| (key.trim().to_string(), value.trim().to_string()))
        .collect::<BTreeMap<_, _>>();
    let page_count = fields
        .get("Pages")
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            ContentError::Invalid(
                "PDF backend did not return a valid physical page count".to_string(),
            )
        })?;
    if page_count > MAX_PAGES {
        return Err(ContentError::Invalid(format!(
            "PDF exceeds {MAX_PAGES}-page extraction limit"
        )));
    }
    if fields
        .get("Encrypted")
        .is_some_and(|value| value.starts_with("yes"))
    {
        return Err(ContentError::Invalid(
            "encrypted PDF is not extracted; provide an accessible unencrypted source".to_string(),
        ));
    }
    let images = if let Some(program) = &capabilities.pdfimages {
        Some(run(
            program,
            &["-list".into(), input.as_os_str().to_os_string()],
            scratch,
            "images",
            deadline,
            cancelled,
        )?)
    } else {
        None
    };
    let mut image_pages = std::collections::BTreeSet::new();
    if let Some(images) = &images {
        for line in images.lines() {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            if fields.len() > 2
                && let Ok(page) = fields[0].parse::<u32>()
            {
                image_pages.insert(page);
            }
        }
    }
    let mut diagnostics = Vec::new();
    let mut segments = Vec::new();
    let mut native_pages = Vec::new();
    let mut native_first = 1;
    let mut native_end = 0;
    for page in 1..=page_count {
        if let Some(existing) = staged.iter().find(|segment| segment.ordinal == page) {
            if existing.coverage != "complete" {
                diagnostics.push(format!(
                    "page {page} resumed with {} coverage from {} extraction; page requires review",
                    existing.coverage, existing.method
                ));
            }
            segments.push(existing.clone());
            continue;
        }
        if Instant::now() >= deadline
            || cancelled.load(Ordering::Relaxed)
            || super::check_content_cancellation().is_err()
        {
            return Err(ContentError::Invalid(
                "PDF extraction deadline exceeded; queued work can be retried".to_string(),
            ));
        }
        if page > native_end {
            native_first = page;
            native_end = (page + 31).min(page_count);
            native_pages = native_batch(NativeBatchRequest {
                program: pdftotext,
                input: &input,
                scratch,
                first: native_first,
                last: native_end,
                deadline,
                cancelled,
            })
            .unwrap_or_default();
        }
        let output = scratch.join("native.txt");
        let native = if let Some(text) = native_pages.get((page - native_first) as usize) {
            Ok(text.clone())
        } else {
            // Oversized output, backend errors or ambiguous page separators fall
            // back to exact single-page extraction; no physical pages are guessed.
            run(
                pdftotext,
                &[
                    "-f".into(),
                    page.to_string().into(),
                    "-l".into(),
                    page.to_string().into(),
                    "-layout".into(),
                    "-enc".into(),
                    "UTF-8".into(),
                    "-nopgbrk".into(),
                    input.as_os_str().to_os_string(),
                    output.as_os_str().to_os_string(),
                ],
                scratch,
                "native",
                deadline,
                cancelled,
            )
            .and_then(|_| read_bounded(&output))
        };
        let (mut text, mut coverage) = match native {
            Ok(text) => (text, "complete".to_string()),
            Err(error) => {
                diagnostics.push(format!("page {page} native extraction: {error}"));
                (String::new(), "partial".to_string())
            }
        };
        let mut method = "native".to_string();
        // Native headings do not prove image-body coverage. Image-bearing pages require OCR.
        let needs_ocr = text.contains('\u{fffd}')
            || text.chars().any(|ch| matches!(ch as u32, 0xe000..=0xf8ff))
            || text.trim().is_empty()
            || image_pages.contains(&page)
            || images.is_none();
        if needs_ocr {
            if let (Some(renderer), Some(ocr), Some(model)) = (
                &capabilities.pdftoppm,
                &capabilities.tesseract,
                &capabilities.ocr_model,
            ) {
                match ocr_page(OcrRequest {
                    renderer,
                    ocr,
                    model,
                    input: &input,
                    page,
                    scratch,
                    deadline,
                    cancelled,
                }) {
                    Ok((ocr_text, confidence, blank)) => {
                        if blank && text.trim().is_empty() {
                            text.clear();
                            coverage = "complete".to_string();
                            method = "rendered_blank".to_string();
                        } else if !ocr_text.trim().is_empty() {
                            let (selected, selected_method) = select_text(&text, &ocr_text);
                            let combined = selected_method == "native+ocr";
                            text = selected;
                            method = selected_method.to_string();
                            coverage = if confidence >= 70.0 && !combined {
                                "complete"
                            } else {
                                "partial"
                            }
                            .to_string();
                            if coverage != "complete" {
                                diagnostics.push(format!("page {page} OCR requires review (mean confidence {confidence:.1}, merged={combined})"));
                            }
                        } else {
                            coverage = "needs_ocr".to_string();
                            diagnostics.push(format!(
                                "page {page} has nonblank visual content but no reliable OCR text"
                            ));
                        }
                    }
                    Err(error) => {
                        coverage = "needs_ocr".to_string();
                        diagnostics
                            .push(format!("page {page} local OCR unavailable/failed: {error}"));
                    }
                }
            } else {
                coverage = "needs_ocr".to_string();
                diagnostics.push(format!("page {page} requires local OCR; provision pdftoppm, Tesseract and the eng model"));
            }
        }
        if text
            .lines()
            .any(|line| line.trim().contains("  ") || line.contains('\t'))
        {
            coverage = "partial".to_string();
            diagnostics.push(format!(
                "page {page} has table/column spacing; plain-text reading order requires review"
            ));
        }
        if text.len() > MAX_SEGMENT_BYTES {
            text.truncate(previous_boundary(&text, MAX_SEGMENT_BYTES));
            coverage = "partial".to_string();
            diagnostics.push(format!("page {page} text truncated at 256 KiB"));
        }
        let segment = ContentSegmentRecord {
            file_id: job.file_id.clone(),
            ordinal: page,
            locator_kind: "page".to_string(),
            source_start: page,
            source_end: page,
            text,
            method,
            coverage,
        };
        on_page(&segment)?;
        segments.push(segment);
        for name in ["native.txt", "page.pgm", "ocr.tsv"] {
            let _ = fs::remove_file(scratch.join(name));
        }
    }
    let coverage = if segments
        .iter()
        .all(|segment| segment.coverage == "complete")
    {
        "complete"
    } else if segments
        .iter()
        .all(|segment| segment.text.trim().is_empty())
    {
        "needs_ocr"
    } else {
        "partial"
    };
    let metadata = json!({"page_count":page_count,"page_ordinals":"physical_one_based","pdf_metadata":fields,"metadata_provenance":"poppler_pdfinfo","backend":capabilities.backend,"ocr_languages":capabilities.ocr_languages,"ocr_model_revision":capabilities.ocr_model_revision,"limits":{"max_input_bytes":super::MAX_PDF_BYTES,"max_render_side":2048,"max_scratch_bytes":MAX_SCRATCH_BYTES,"memory_limit_enforced":capabilities.worker_memory_limit_enforced,"memory_watchdog":capabilities.worker_memory_watchdog,"memory_limit_bytes":capabilities.worker_memory_limit_bytes}});
    Ok(ExtractedPdf {
        segments,
        coverage: coverage.to_string(),
        metadata,
        diagnostics,
    })
}

struct NativeBatchRequest<'a> {
    program: &'a Path,
    input: &'a Path,
    scratch: &'a Path,
    first: u32,
    last: u32,
    deadline: Instant,
    cancelled: &'a AtomicBool,
}

fn native_batch(request: NativeBatchRequest<'_>) -> Result<Vec<String>, ContentError> {
    let NativeBatchRequest {
        program,
        input,
        scratch,
        first,
        last,
        deadline,
        cancelled,
    } = request;
    let output = scratch.join("native.txt");
    run(
        program,
        &[
            "-f".into(),
            first.to_string().into(),
            "-l".into(),
            last.to_string().into(),
            "-layout".into(),
            "-enc".into(),
            "UTF-8".into(),
            input.as_os_str().to_os_string(),
            output.as_os_str().to_os_string(),
        ],
        scratch,
        "native",
        deadline,
        cancelled,
    )?;
    split_native_pages(&read_bounded(&output)?, (last - first + 1) as usize)
}

fn split_native_pages(text: &str, expected: usize) -> Result<Vec<String>, ContentError> {
    let body = text
        .strip_suffix('\u{c}')
        .ok_or_else(|| ContentError::Invalid("native page terminator missing".to_string()))?;
    let pages = body
        .split('\u{c}')
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if pages.len() != expected {
        return Err(ContentError::Invalid(
            "native physical page count mismatch".to_string(),
        ));
    }
    Ok(pages)
}

struct OcrRequest<'a> {
    renderer: &'a Path,
    ocr: &'a Path,
    model: &'a Path,
    input: &'a Path,
    page: u32,
    scratch: &'a Path,
    deadline: Instant,
    cancelled: &'a AtomicBool,
}
fn ocr_page(request: OcrRequest<'_>) -> Result<(String, f64, bool), ContentError> {
    let OcrRequest {
        renderer,
        ocr,
        model,
        input,
        page,
        scratch,
        deadline,
        cancelled,
    } = request;
    let prefix = scratch.join("page");
    run(
        renderer,
        &[
            "-f".into(),
            page.to_string().into(),
            "-l".into(),
            page.to_string().into(),
            "-singlefile".into(),
            "-scale-to".into(),
            "2048".into(),
            "-gray".into(),
            input.as_os_str().to_os_string(),
            prefix.as_os_str().to_os_string(),
        ],
        scratch,
        "render",
        deadline,
        cancelled,
    )?;
    let image = scratch.join("page.pgm");
    if is_white_pgm(&image)? {
        return Ok((String::new(), 100.0, true));
    }
    let output = scratch.join("ocr");
    run(
        ocr,
        &[
            image.as_os_str().to_os_string(),
            output.as_os_str().to_os_string(),
            "--tessdata-dir".into(),
            model
                .parent()
                .unwrap_or(Path::new("."))
                .as_os_str()
                .to_os_string(),
            "-l".into(),
            OCR_LANGUAGE.into(),
            "--psm".into(),
            "3".into(),
            "tsv".into(),
        ],
        scratch,
        "ocr",
        deadline,
        cancelled,
    )?;
    let tsv = read_bounded(&scratch.join("ocr.tsv"))?;
    let mut words = Vec::new();
    let mut confidence = 0.0;
    let mut count = 0u32;
    let mut previous = String::new();
    for line in tsv.lines().skip(1) {
        let fields = line.splitn(12, '\t').collect::<Vec<_>>();
        if fields.len() != 12 || fields[0] != "5" || fields[11].trim().is_empty() {
            continue;
        }
        let key = fields[1..5].join("/");
        if !words.is_empty() {
            words.push(if key == previous {
                " ".to_string()
            } else {
                "\n".to_string()
            });
        }
        words.push(fields[11].to_string());
        previous = key;
        confidence += fields[10].parse::<f64>().unwrap_or(0.0).max(0.0);
        count += 1;
    }
    Ok((
        words.concat(),
        if count > 0 {
            confidence / f64::from(count)
        } else {
            0.0
        },
        false,
    ))
}

fn select_text(native: &str, ocr: &str) -> (String, &'static str) {
    if native.trim().is_empty() {
        return (ocr.to_string(), "ocr");
    }
    let normalize = |value: &str| {
        value
            .split_whitespace()
            .map(str::to_lowercase)
            .collect::<Vec<_>>()
            .join(" ")
    };
    let native_normalized = normalize(native);
    let ocr_normalized = normalize(ocr);
    if ocr_normalized.contains(&native_normalized) {
        return (ocr.to_string(), "ocr");
    }
    if native_normalized.contains(&ocr_normalized) {
        return (native.to_string(), "native");
    }
    let mut lines = native.lines().map(ToString::to_string).collect::<Vec<_>>();
    for line in ocr.lines() {
        if !lines
            .iter()
            .any(|existing| normalize(existing) == normalize(line))
        {
            lines.push(line.to_string());
        }
    }
    (lines.join("\n"), "native+ocr")
}

fn previous_boundary(text: &str, mut at: usize) -> usize {
    while !text.is_char_boundary(at) {
        at -= 1;
    }
    at
}

fn read_bounded(path: &Path) -> Result<String, ContentError> {
    let bytes = read_bytes_bounded(path, MAX_COMMAND_OUTPUT)?;
    String::from_utf8(bytes)
        .map_err(|_| ContentError::Invalid("extractor output is not valid UTF-8".to_string()))
}

fn read_bytes_bounded(path: &Path, limit: u64) -> Result<Vec<u8>, ContentError> {
    let file = File::open(path)?;
    if file.metadata()?.len() > limit {
        return Err(ContentError::Invalid(
            "extractor output exceeds byte limit".to_string(),
        ));
    }
    let mut bytes = Vec::new();
    file.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(ContentError::Invalid(
            "extractor output exceeds byte limit".to_string(),
        ));
    }
    Ok(bytes)
}

/// P5 white-pixel verification is deliberately conservative: faint marks are not blank.
fn is_white_pgm(path: &Path) -> Result<bool, ContentError> {
    let bytes = read_bytes_bounded(path, 8 * 1024 * 1024)?;
    let mut at = 0;
    let mut tokens = Vec::new();
    while tokens.len() < 4 && at < bytes.len() {
        while at < bytes.len() && bytes[at].is_ascii_whitespace() {
            at += 1;
        }
        if at < bytes.len() && bytes[at] == b'#' {
            while at < bytes.len() && bytes[at] != b'\n' {
                at += 1;
            }
            continue;
        }
        let start = at;
        while at < bytes.len() && !bytes[at].is_ascii_whitespace() {
            at += 1;
        }
        tokens.push(String::from_utf8_lossy(&bytes[start..at]).to_string());
    }
    if tokens.len() != 4 || tokens[0] != "P5" || tokens[3] != "255" {
        return Err(ContentError::Invalid(
            "unexpected renderer pixel format".to_string(),
        ));
    }
    let width = tokens[1].parse::<usize>().unwrap_or(0);
    let height = tokens[2].parse::<usize>().unwrap_or(0);
    if width == 0 || height == 0 || width > 2048 || height > 2048 {
        return Err(ContentError::Invalid(
            "rendered page dimensions exceed limit".to_string(),
        ));
    }
    if at < bytes.len() {
        at += 1;
    }
    let pixels = &bytes[at..];
    Ok(pixels.len() == width * height && pixels.iter().all(|value| *value == 255))
}

/// Only direct trusted program paths are spawned. Output goes to capped scratch, never pipes.
fn run(
    program: &Path,
    args: &[OsString],
    scratch: &Path,
    label: &str,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> Result<String, ContentError> {
    if Instant::now() >= deadline
        || cancelled.load(Ordering::Relaxed)
        || super::check_content_cancellation().is_err()
    {
        return Err(ContentError::Invalid(
            "extraction deadline exceeded".to_string(),
        ));
    }
    let stdout = scratch.join(format!("{label}.stdout"));
    let stderr = scratch.join(format!("{label}.stderr"));
    let mut command = bounded_command(program);
    let child = command
        .args(args)
        .env("OMP_THREAD_LIMIT", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::from(File::create(&stdout)?))
        .stderr(Stdio::from(File::create(&stderr)?))
        .spawn()?;
    let mut child = ChildGuard(child);
    let mut last_memory_check = Instant::now();
    let started = Instant::now();
    loop {
        let status = child.0.try_wait()?;
        let memory_exceeded = if last_memory_check.elapsed() >= Duration::from_millis(50) {
            last_memory_check = Instant::now();
            resident_bytes(child.0.id()).is_some_and(|bytes| bytes > 512 * 1024 * 1024)
        } else {
            false
        };
        let quota = directory_bytes(scratch);
        if Instant::now() >= deadline
            || cancelled.load(Ordering::Relaxed)
            || super::check_content_cancellation().is_err()
            || memory_exceeded
            || quota.as_ref().is_err()
            || quota.unwrap_or(u64::MAX) > MAX_SCRATCH_BYTES
            || fs::metadata(&stdout)?.len() > MAX_COMMAND_OUTPUT
            || fs::metadata(&stderr)?.len() > MAX_COMMAND_OUTPUT
        {
            let _ = child.0.kill();
            let _ = child.0.wait();
            return Err(ContentError::Invalid(
                "extractor exceeded deadline, memory, or scratch/output quota".to_string(),
            ));
        }
        // A fast process can exit between polls; exit does not waive any quota or deadline.
        if let Some(status) = status {
            let output = read_bounded(&stdout)?;
            if status.success() {
                return Ok(output);
            }
            let mut error =
                read_bounded(&stderr).unwrap_or_else(|_| "error output unavailable".to_string());
            error.truncate(previous_boundary(&error, error.len().min(4096)));
            return Err(ContentError::Invalid(format!(
                "{} failed: {}",
                program.file_name().unwrap_or_default().to_string_lossy(),
                error.trim()
            )));
        }
        thread::sleep(Duration::from_millis(
            if started.elapsed() < Duration::from_millis(40) {
                2
            } else {
                15
            },
        ));
    }
}

struct ChildGuard(std::process::Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn find_model(program: &Path) -> Option<PathBuf> {
    let mut directories = Vec::new();
    if let Some(path) = std::env::var_os("TESSDATA_PREFIX") {
        directories.push(PathBuf::from(path));
    }
    if let Some(prefix) = program.parent().and_then(Path::parent) {
        directories.push(prefix.join("share/tessdata"));
    }
    directories.extend(
        [
            "/usr/share/tesseract-ocr/5/tessdata",
            "/usr/share/tessdata",
            "/usr/local/share/tessdata",
        ]
        .into_iter()
        .map(PathBuf::from),
    );
    directories
        .into_iter()
        .map(|path| path.join("eng.traineddata"))
        .find(|path| path.is_file())
        .and_then(|path| path.canonicalize().ok())
}
fn model_revision(path: &Path) -> Option<String> {
    static CACHE: OnceLock<Mutex<Option<(String, String)>>> = OnceLock::new();
    let metadata = fs::metadata(path).ok()?;
    let key = format!(
        "{:?}:{:?}:{}",
        path,
        metadata.modified().ok()?,
        metadata.len()
    );
    let mut cache = CACHE.get_or_init(|| Mutex::new(None)).lock().ok()?;
    if let Some((cached, revision)) = &*cache
        && cached == &key
    {
        return Some(revision.clone());
    }
    let revision = super::hash_bounded(path, 32 * 1024 * 1024).ok()?;
    *cache = Some((key, revision.clone()));
    Some(revision)
}

fn bounded_command(program: &Path) -> Command {
    if cfg!(target_os = "linux") {
        let mut command = Command::new("/bin/sh");
        command
            .args([
                "-c",
                "ulimit -v 1048576 || exit 125; exec \"$@\"",
                "tao-content-worker",
            ])
            .arg(program);
        command
    } else {
        Command::new(program)
    }
}
fn resident_bytes(pid: u32) -> Option<u64> {
    if cfg!(unix) {
        let output = Command::new("/bin/ps")
            .args(["-o", "rss=", "-p", &pid.to_string()])
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output()
            .ok()?;
        if output.status.success() {
            String::from_utf8(output.stdout)
                .ok()?
                .trim()
                .parse::<u64>()
                .ok()
                .map(|kilobytes| kilobytes * 1024)
        } else {
            None
        }
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn scratch() -> tempfile::TempDir {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/content-worker-tests");
        fs::create_dir_all(&root).unwrap();
        tempfile::tempdir_in(root).unwrap()
    }
    #[test]
    fn deadlines_and_cancellation_kill_and_reap_the_direct_child() {
        let directory = scratch();
        let cancelled = AtomicBool::new(false);
        let start = Instant::now();
        assert!(
            run(
                Path::new("/bin/sleep"),
                &["5".into()],
                directory.path(),
                "deadline",
                Instant::now() + Duration::from_millis(30),
                &cancelled
            )
            .is_err()
        );
        assert!(start.elapsed() < Duration::from_secs(1));
        std::thread::scope(|scope| {
            scope.spawn(|| {
                thread::sleep(Duration::from_millis(30));
                cancelled.store(true, Ordering::Relaxed);
            });
            let start = Instant::now();
            assert!(
                run(
                    Path::new("/bin/sleep"),
                    &["5".into()],
                    directory.path(),
                    "cancel",
                    Instant::now() + Duration::from_secs(6),
                    &cancelled
                )
                .is_err()
            );
            assert!(start.elapsed() < Duration::from_secs(1));
        });
    }
    #[test]
    fn output_quota_is_enforced_even_when_process_exits_between_polls() {
        let directory = scratch();
        let input = directory.path().join("large-output");
        fs::write(&input, vec![b'x'; MAX_COMMAND_OUTPUT as usize + 1]).unwrap();
        assert!(
            run(
                Path::new("/bin/cat"),
                &[input.into_os_string()],
                directory.path(),
                "quota",
                Instant::now() + Duration::from_secs(3),
                &AtomicBool::new(false)
            )
            .is_err()
        );
    }
    #[test]
    #[cfg(unix)]
    fn fast_success_does_not_bypass_stderr_or_scratch_quotas() {
        let directory = scratch();
        assert!(
            run(
                Path::new("/bin/sh"),
                &[
                    "-c".into(),
                    "dd if=/dev/zero bs=1048576 count=5 >&2 2>/dev/null; exit 0".into()
                ],
                directory.path(),
                "stderr-quota",
                Instant::now() + Duration::from_secs(3),
                &AtomicBool::new(false)
            )
            .is_err()
        );
        let large = directory.path().join("render.pgm");
        File::create(&large)
            .unwrap()
            .set_len(MAX_SCRATCH_BYTES + 1)
            .unwrap();
        assert!(
            run(
                Path::new("/usr/bin/true"),
                &[],
                directory.path(),
                "scratch-quota",
                Instant::now() + Duration::from_secs(3),
                &AtomicBool::new(false)
            )
            .is_err()
        );
        assert!(is_white_pgm(&large).is_err());
    }
    #[test]
    fn native_ocr_selection_avoids_duplicate_text_and_preserves_uncertain_merges() {
        assert_eq!(
            select_text("Heading", "Heading\nBody evidence"),
            ("Heading\nBody evidence".to_string(), "ocr")
        );
        assert_eq!(
            select_text("Heading\nBody evidence", "Heading"),
            ("Heading\nBody evidence".to_string(), "native")
        );
        let (merged, method) = select_text("Heading", "Body evidence");
        assert_eq!(method, "native+ocr");
        assert_eq!(merged, "Heading\nBody evidence");
    }

    #[test]
    fn native_batches_require_exact_physical_page_boundaries() {
        assert_eq!(
            split_native_pages("first\n\u{c}\u{c}third\n\u{c}", 3).unwrap(),
            vec!["first\n", "", "third\n"]
        );
        assert!(split_native_pages("first\n\u{c}third\n\u{c}", 3).is_err());
        assert!(split_native_pages("no page terminator", 1).is_err());
        assert!(split_native_pages("embedded\u{c}separator\u{c}", 1).is_err());
    }
}
