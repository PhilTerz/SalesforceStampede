# Stampede — Implementation Guide

This guide breaks the architecture into discrete implementation chunks. Each chunk is designed to be:
- **Self-contained**: Can be implemented in one AI session
- **Testable**: Has clear success criteria
- **Sequential**: Dependencies are explicit

## Repository Setup

### Initial Setup (Do This First, Manually)

```bash
# Create project
bun create tauri-app
# Name: stampede
# Frontend: TypeScript / JavaScript → Svelte → TypeScript

cd stampede

# Install dependencies
bun install

# Verify it runs
bun run tauri dev
```

### Directory Structure to Create

```
src-tauri/src/
├── main.rs           # Entry point (generated)
├── lib.rs            # Module exports
├── commands/         # Tauri IPC commands
├── salesforce/       # SF API client
├── streaming/        # CSV chunking
├── validation/       # CSV validation
├── storage/          # SQLite + keychain
└── error.rs          # Error types
```

---

## Implementation Chunks

### Phase 1: Foundation

#### Chunk 1.1: Error Types & App State Shell
**Estimated time**: 30 min  
**Dependencies**: None  
**Files to create**: `error.rs`, `lib.rs`

**Prompt for AI**:
> Create the error types for a Salesforce desktop app. Include errors for: authentication (NotAuthenticated, SessionExpired, OAuthError), API (SalesforceError, RateLimited, ConcurrentLimitExceeded, CountTimeout), bulk operations (JobFailed, Cancelled), file handling (NotUtf8, CsvInvalid, CsvChunkError), and network (ConnectionFailed). Use thiserror. Also create an ErrorPresentation struct that maps errors to user-friendly messages with suggested actions.

**Success criteria**:
- `cargo check` passes
- All error variants defined
- `to_presentation()` method works

---

#### Chunk 1.2: SQLite Schema & Database Module
**Estimated time**: 45 min  
**Dependencies**: Chunk 1.1  
**Files to create**: `storage/mod.rs`, `storage/database.rs`

**Prompt for AI**:
> Create a SQLite database module using rusqlite for a Salesforce desktop app. Include tables for: orgs (id, name, org_type, instance_url, login_url, username, api_version), job_groups (tracking bulk upload jobs with batch_size, total_parts, records_processed), jobs (individual SF job tracking), saved_queries, and query_history. Include migrations and a Database struct with init() that creates tables if they don't exist. Use spawn_blocking for async compatibility.

**Success criteria**:
- Database creates on first run
- Tables exist with correct schema
- Can insert/query test data

---

#### Chunk 1.3: Keychain Credential Storage
**Estimated time**: 30 min  
**Dependencies**: Chunk 1.1  
**Files to create**: `storage/credentials.rs`

**Prompt for AI**:
> Create a credential storage module using the `keyring` crate for a Salesforce app. Store OAuth tokens (access_token, refresh_token) as JSON per org. Include methods: store_tokens(org_id, access, refresh), get_tokens(org_id) -> Result<TokenPair>, delete_tokens(org_id). The service name should be "salesforce-power-toolkit". Handle keyring errors gracefully.

**Success criteria**:
- Can store and retrieve tokens
- Tokens not logged (Debug impl redacts)
- Works on macOS and Windows

---

#### Chunk 1.4: Base HTTP Client with Safe Logging
**Estimated time**: 45 min  
**Dependencies**: Chunk 1.3  
**Files to create**: `salesforce/mod.rs`, `salesforce/client.rs`

**Prompt for AI**:
> Create a base Salesforce HTTP client using reqwest. Requirements:
> 1. Custom User-Agent: "SalesforcePowerToolkit/1.0.0"
> 2. 300s timeout
> 3. SafeLoggingClient wrapper that logs only: method, URL path (no query params), status, duration, x-request-id header. NEVER log bodies or auth headers.
> 4. Store credentials in Arc<RwLock<OrgCredentials>>
> 5. Include refresh_lock: Arc<Mutex<()>> for token refresh synchronization
> 
> Don't implement OAuth yet - just the base client structure.

**Success criteria**:
- Client compiles
- Logging doesn't leak sensitive data
- Can make unauthenticated requests

---

#### Chunk 1.5: OAuth Flow with Port Fallback
**Estimated time**: 1 hour  
**Dependencies**: Chunk 1.4  
**Files to create**: `salesforce/auth.rs`

**Prompt for AI**:
> Implement OAuth 2.0 Web Server flow for Salesforce with PKCE. Requirements:
> 1. Port fallback ladder: try ports [8477, 8478, 8479, 9876, 0] until one binds
> 2. Support login URLs: login.salesforce.com, test.salesforce.com, custom My Domain
> 3. Start local HTTP server, open browser to auth URL, wait for callback
> 4. Exchange code for tokens, store in keychain
> 5. Return OrgCredentials with instance_url, user_id, username, org_id
>
> Use the oauth2 crate. Include proper PKCE challenge/verifier.

**Success criteria**:
- Can authenticate to a Salesforce org
- Tokens stored in keychain
- Port fallback works when preferred port is taken

---

#### Chunk 1.6: Token Refresh Gate
**Estimated time**: 30 min  
**Dependencies**: Chunk 1.5  
**Files to create**: Update `salesforce/client.rs`

**Prompt for AI**:
> Add token refresh logic to the Salesforce client. Implement a single-gate pattern:
> 1. `get_valid_access_token()` - THE ONLY way to get a token. Checks expiry (with 60s buffer), refreshes if needed.
> 2. Use refresh_lock mutex to prevent concurrent refreshes
> 3. After acquiring lock, re-check token (another thread may have refreshed)
> 4. `execute_with_retry()` - wraps requests, calls get_valid_access_token(), retries once on 401
>
> All request methods must go through execute_with_retry().

**Success criteria**:
- Expired tokens refresh automatically
- Concurrent requests don't cause refresh storms
- 401 responses trigger one retry

---

#### Chunk 1.7: Tauri Commands - Auth
**Estimated time**: 30 min  
**Dependencies**: Chunk 1.6  
**Files to create**: `commands/mod.rs`, `commands/auth.rs`

**Prompt for AI**:
> Create Tauri IPC commands for authentication:
> 1. `login(login_type: LoginType)` - initiates OAuth flow, returns OrgCredentials
> 2. `logout(org_id: String)` - removes tokens from keychain, removes from DB
> 3. `list_orgs()` - returns all saved orgs from SQLite
> 4. `switch_org(org_id: String)` - sets active org in AppState
>
> LoginType enum: Production, Sandbox, CustomDomain { domain: String }
> Store AppState in Tauri's managed state.

**Success criteria**:
- Can login from frontend
- Orgs persist across app restarts
- Can switch between orgs

---

### Phase 2: Query Execution

#### Chunk 2.1: REST API Query
**Estimated time**: 45 min  
**Dependencies**: Chunk 1.6  
**Files to create**: `salesforce/rest.rs`

**Prompt for AI**:
> Implement Salesforce REST API query execution:
> 1. `query(soql: &str)` - executes query, handles pagination via nextRecordsUrl
> 2. `query_with_limit(soql: &str, max_rows: u64)` - stops after max_rows even if more available
> 3. Return QueryResult { records: Vec<serde_json::Value>, total_size: u64, done: bool }
> 4. Parse Salesforce error responses into AppError::SalesforceError
>
> Use the /services/data/vXX.0/query endpoint.

**Success criteria**:
- Can execute SOQL queries
- Pagination works for large result sets
- Errors parsed correctly

---

#### Chunk 2.2: Query Strategy (COUNT + Auto-select)
**Estimated time**: 45 min  
**Dependencies**: Chunk 2.1  
**Files to create**: `salesforce/query_strategy.rs`

**Prompt for AI**:
> Implement query strategy selection:
> 1. `count_query(soql: &str, timeout_secs: u64)` - wraps query in COUNT(), returns count or CountTimeout error
> 2. QueryStrategy enum: Auto, Rest, Bulk
> 3. `determine_strategy(soql, count_result)`:
>    - count ≤ 2000 → Rest (fast)
>    - 2000 < count ≤ 50000 → Rest (to grid)
>    - count > 50000 → Bulk (export to file)
>    - timeout → prompt user
> 4. Default COUNT timeout: 5 seconds
>
> Make timeout configurable via QueryPreferences struct.

**Success criteria**:
- COUNT works with timeout
- Strategy selection matches thresholds
- Timeout returns appropriate error

---
#### Chunk 2.3: Bulk API v2 Query Client (Export)
**Estimated time**: 1 hour  
**Dependencies**: Chunk 1.6  
**Files to create**: `salesforce/bulk_query_v2.rs`

**Prompt for AI**:
> Implement Salesforce Bulk API v2 **query/export** operations:
> 1. `create_query_job(soql: &str)` → returns job_id
> 2. `get_query_job_status(job_id)` → returns state + processed/failed counts when available
> 3. `download_query_results(job_id, output_path)`:
>    - Streams CSV results to disk (do not load into memory)
>    - Handles result pagination via locator (if applicable)
> 4. `abort_query_job(job_id)` - best-effort abort
> 
> Use the `/services/data/vXX.0/jobs/query` endpoints. Parse Salesforce errors into AppError variants, especially:
> - RateLimited (capture Retry-After if present)
> - ConcurrentLimitExceeded
> - JobNotFound / JobFailed

**Success criteria**:
- Can export a query to a CSV file from a real org
- Export streams to disk (memory stays flat for large exports)
- Errors map to user-friendly AppError variants

---


#### Chunk 2.4: Tauri Commands - Query
**Estimated time**: 30 min  
**Dependencies**: Chunks 2.2, 2.3  
**Files to create**: `commands/query.rs`

**Prompt for AI**:
> Create Tauri commands for query execution:
> 1. `execute_query(soql: String, strategy: QueryStrategy)` - returns QueryResult
>    - If strategy is Bulk, run Bulk API v2 query export and return `{ export_path, exported: true }` (no grid rows)
> 2. `save_query(name: String, soql: String)` - saves to SQLite
> 3. `get_saved_queries()` - returns saved queries for current org
> 4. `get_query_history(limit: u32)` - returns recent queries
>
> Emit progress events during pagination: "query-progress" { fetched, total }
> Enforce 50K row grid limit - return truncated flag if more available.

**Success criteria**:
- REST queries execute and return results
- Progress events fire during pagination
- Results truncate at 50K rows
- Bulk strategy exports to a CSV file and returns the output path

---

### Phase 3: CSV Handling

#### Chunk 3.1: CSV Validation
**Estimated time**: 45 min  
**Dependencies**: Chunk 1.1  
**Files to create**: `validation/mod.rs`, `validation/csv_validator.rs`

**Prompt for AI**:
> Create CSV validation module (UTF-8 only):
> 1. `validate(path: &Path)` returns CsvValidationResult
> 2. Check UTF-8 validity (first 8KB sample)
> 3. Detect BOM, line endings (LF/CRLF/Mixed)
> 4. Parse headers, check column count consistency (first 1000 rows only)
> 5. Estimate total row count from file size
> 6. Return errors: NotUtf8, EmptyFile, NoHeaders, InconsistentColumns
> 7. Return warnings: HasBom, MixedLineEndings, LargeFile, SampleOnlyValidation
>
> Use the `csv` crate for parsing.

**Success criteria**:
- Detects non-UTF-8 files
- Catches column count mismatches
- Large files validate quickly (sample-based)

---

#### Chunk 3.2: CSV Chunking (Record-Aware)
**Estimated time**: 1 hour  
**Dependencies**: Chunk 3.1  
**Files to create**: `streaming/mod.rs`, `streaming/csv_chunker.rs`

**Prompt for AI**:
> Create CSV chunker that correctly handles quoted fields with embedded newlines:
> 1. Use `csv` crate to read records (NOT line-based splitting)
> 2. ChunkConfig { max_bytes: 100MB, max_records: configurable }
> 3. `split_file(source, temp_dir, config)` returns ChunkResult { chunk_paths, total_rows, rows_per_chunk }
> 4. Each chunk gets the header row
> 5. Start new chunk when EITHER byte limit OR record limit exceeded
> 6. BatchSize enum: ExtraSmall(10), Small(200), Medium(2000), Large(10000), Custom(u32)
>
> Critical: must not corrupt rows with embedded newlines.

**Success criteria**:
- Chunking preserves data integrity
- Embedded newlines handled correctly
- Batch size limits respected

**Test case to include**:
```rust
// CSV with embedded newline must stay intact
let csv = r#"Name,Desc
"John","Line1
Line2"
"#;
```

---

#### Chunk 3.3: Atomic File Writer
**Estimated time**: 20 min  
**Dependencies**: None  
**Files to create**: `streaming/atomic_writer.rs`

**Prompt for AI**:
> Create an atomic CSV writer that uses temp file + rename:
> 1. AtomicCsvWriter::new(final_path) - creates temp file in same directory
> 2. Wraps csv::Writer with BufWriter (64KB buffer)
> 3. finish() - flushes, drops writer, persists temp file to final path
> 4. Drop impl cleans up temp file if not finished
>
> Use the `tempfile` crate's NamedTempFile.

**Success criteria**:
- Incomplete writes don't corrupt target file
- Works on Windows (same-volume rename)

---

### Phase 4: Bulk API

#### Chunk 4.1: Bulk API v2 Client
**Estimated time**: 1 hour  
**Dependencies**: Chunk 1.6  
**Files to create**: `salesforce/bulk_v2.rs`

**Prompt for AI**:
> Implement Salesforce Bulk API v2 ingest operations:
> 1. `create_ingest_job(object, operation)` - creates job, returns job_id
> 2. `upload_job_data(job_id, csv_path)` - streams CSV via PUT
> 3. `close_job(job_id)` - marks upload complete
> 4. `get_job_status(job_id)` - returns state, records processed/failed
> 5. `abort_job(job_id)` - best-effort abort
> 6. `get_success_results(job_id)` - downloads success CSV
> 7. `get_failure_results(job_id)` - downloads failure CSV
>
> Operations: Insert, Update, Upsert, Delete
> Handle API errors, especially ConcurrentLimitExceeded.

**Success criteria**:
- Can create and upload to bulk jobs
- Streaming upload doesn't load file into memory
- Job status polling works

---

#### Chunk 4.2: Bulk Job Scheduler (Concurrency Control)
**Estimated time**: 30 min  
**Dependencies**: Chunk 4.1  
**Files to create**: `salesforce/bulk_scheduler.rs`

**Prompt for AI**:
> Create a bulk job scheduler with concurrency limiting:
> 1. BulkJobScheduler with Semaphore (max 3 concurrent jobs)
> 2. `acquire()` - blocks until permit available, returns BulkJobPermit
> 3. `try_acquire()` - non-blocking, returns Option<BulkJobPermit>
> 4. `active_jobs()` - returns count of active jobs
> 5. `available_slots()` - returns remaining permits
> 6. BulkJobPermit releases on drop
>
> Use tokio::sync::Semaphore.

**Success criteria**:
- No more than 3 concurrent bulk jobs
- UI can show active/available counts

---

#### Chunk 4.3: Job Persistence (SQLite)
**Estimated time**: 45 min  
**Dependencies**: Chunk 1.2, Chunk 4.1  
**Files to create**: `storage/jobs.rs`

**Prompt for AI**:
> Create job persistence layer:
> 1. `save_group(BulkJobGroup)` - inserts job group
> 2. `add_job_to_group(group_id, job_id, part_number)`
> 3. `update_job_state(job_id, state)`
> 4. `get_group(group_id)` - returns group with all jobs
> 5. `get_active_groups()` - non-terminal groups
> 6. `reconcile_jobs(client)` - sync with SF on startup (throttled: batch of 10, 500ms delay)
> 7. `cleanup_old_jobs(retention_days)` - delete completed jobs older than N days
>
> BulkJobGroup tracks: group_id, org_id, object, operation, state, batch_size, total_parts, records_processed/failed

**Success criteria**:
- Jobs survive app restart
- Reconciliation syncs state with Salesforce
- Old jobs cleaned up automatically

---

#### Chunk 4.4: Bulk Upload Orchestration
**Estimated time**: 1 hour  
**Dependencies**: Chunks 4.1, 4.2, 4.3, 3.2  
**Files to create**: `commands/bulk.rs`

**Prompt for AI**:
> Create the bulk upload orchestrator:
> 1. `start_bulk_upload(BulkUploadRequest)`:
>    - Validate CSV
>    - Split into chunks (respecting batch_size)
>    - Create job group in DB
>    - For each chunk: acquire permit, create SF job, upload, close
>    - Emit progress events: "bulk-progress" { group_id, current_part, total_parts, percent }
>    - Start background poller on completion
> 2. Support cancellation via CancellationToken
> 3. CancellationState: CancelledLocal, AbortingRemote, PartiallyCompleted
>
> Cancellation should: stop new chunks, best-effort abort open jobs, allow results download for completed chunks.

**Success criteria**:
- Multi-chunk upload works end-to-end
- Progress events fire correctly
- Cancellation stops gracefully

---

#### Chunk 4.5: Result Merging
**Estimated time**: 30 min  
**Dependencies**: Chunk 4.1  
**Files to create**: `streaming/result_merger.rs`

**Prompt for AI**:
> Create result merger for multi-job bulk operations:
> 1. `download_group_results(group_id, output_dir)` returns BulkGroupResults
> 2. Merge success results from all jobs into one CSV
> 3. Merge failure results into one CSV
> 4. Handle header mismatches: use first job's headers as canonical, log warning
> 5. Handle empty results: create file with headers + comment row
> 6. Write warnings to separate _merge_warnings.txt if any issues
>
> Return paths to success, failure, and optional warnings files.

**Success criteria**:
- Merged CSV is valid
- Header mismatches don't crash
- Empty results produce valid files

---

### Phase 5: Frontend (Svelte)

#### Chunk 5.1: App Shell & Routing
**Estimated time**: 45 min  
**Dependencies**: Phase 1 complete  
**Files to create**: Frontend structure

**Prompt for AI**:
> Create the Svelte app shell with:
> 1. Sidebar with: OrgSwitcher, NavigationMenu (Query, Bulk, Apex, Settings), ActiveJobs indicator
> 2. Main content area with route-based views
> 3. Routes: /query, /bulk, /apex, /settings
> 4. Global stores: currentOrg, activeJobs
> 5. Tauri API integration for invoke() calls
>
> Use Svelte 5 with TypeScript. Keep styling minimal for now.

---

#### Chunk 5.2: Login & Org Management UI
**Estimated time**: 45 min  
**Dependencies**: Chunk 5.1, Chunk 1.7  

**Prompt for AI**:
> Create login and org management UI:
> 1. Login modal with: Production/Sandbox radio, Custom Domain input
> 2. OrgSwitcher dropdown showing all orgs, current org highlighted
> 3. Org management: list orgs, remove org, set default
> 4. Handle OAuth callback (loading state while waiting)
> 5. Error display for failed logins
>
> Wire up to Tauri commands: login, logout, list_orgs, switch_org

---

#### Chunk 5.3: Query Editor & Results Grid
**Estimated time**: 1 hour  
**Dependencies**: Chunk 5.1, Chunk 2.4  

**Prompt for AI**:
> Create query interface:
> 1. Monaco editor for SOQL (basic SQL highlighting for now)
> 2. Toolbar: Run button, Strategy selector (Auto/REST/Bulk), Export button
>    - Bulk strategy = Bulk API v2 export-to-file (v1 requirement)
> 3. AG Grid (Community) for results - max 50K rows
> 4. Truncation warning when results exceed limit
> 5. Progress indicator during query execution
> 6. ID columns as clickable links to Salesforce
>
> Listen for "query-progress" events. Handle errors with ErrorPresentation display.

---

#### Chunk 5.4: Bulk Upload UI
**Estimated time**: 1 hour  
**Dependencies**: Chunk 5.1, Chunk 4.4  

**Prompt for AI**:
> Create bulk upload interface:
> 1. Operation selector: Insert, Update, Upsert, Delete
> 2. Object selector (text input for now, autocomplete later)
> 3. File dropzone with drag-and-drop
> 4. CSV validation display (errors, warnings, stats)
> 5. Batch size selector: ExtraSmall/Small/Medium/Large/Custom with descriptions
> 6. Multi-part progress: overall bar, per-chunk status, record counts
> 7. Concurrency indicator: "Active jobs: 2/3"
> 8. Cancel button with confirmation
> 9. Results download buttons (success, failures)
>
> Listen for "bulk-progress" events.

---

### Phase 6: Polish

#### Chunk 6.1: Query History & Saved Queries UI
#### Chunk 6.2: Anonymous Apex Execution
#### Chunk 6.3: Settings UI (Query Preferences)
#### Chunk 6.4: App Icon & Installers
#### Chunk 6.5: Auto-updater Integration

---

## Implementation Order Summary

```
Week 1-2: Foundation
├── 1.1 Error types
├── 1.2 SQLite schema
├── 1.3 Keychain storage
├── 1.4 Base HTTP client
├── 1.5 OAuth flow
├── 1.6 Token refresh
└── 1.7 Auth commands

Week 2-3: Query + CSV
├── 2.1 REST query
├── 2.2 Query strategy
├── 2.3 Bulk query export client
├── 2.4 Query commands
├── 3.1 CSV validation
├── 3.2 CSV chunking
└── 3.3 Atomic writer

Week 3-4: Bulk API
├── 4.1 Bulk v2 client
├── 4.2 Job scheduler
├── 4.3 Job persistence
├── 4.4 Upload orchestration
└── 4.5 Result merging

Week 4-5: Frontend
├── 5.1 App shell
├── 5.2 Login UI
├── 5.3 Query UI
└── 5.4 Bulk UI

Week 6: Polish
└── 6.x remaining items
```

## Tips for AI Sessions

1. **Start each session with context**: "I'm building Stampede, a Salesforce desktop app with Tauri/Rust/Svelte. Here's the relevant architecture section: [paste]"

2. **Reference the error types**: The AI should use `AppError` variants, not create new error types.

3. **Keep chunks small**: If a chunk feels too big, split it. Better to have working pieces than incomplete features.

4. **Test as you go**: Each chunk should be testable before moving on.

5. **Commit frequently**: One commit per chunk minimum.

## Getting Started Checklist

- [ ] Create repo with `bun create tauri-app`
- [ ] Verify `bun run tauri dev` works
- [ ] Add dependencies to Cargo.toml (copy from architecture doc)
- [ ] Create directory structure
- [ ] Start with Chunk 1.1 (Error Types)
