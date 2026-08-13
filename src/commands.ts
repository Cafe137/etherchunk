import { AsyncQueue, Binary, Chunk, ChunkSplitter, Dates } from 'cafe-utility'
import { once } from 'node:events'
import { createReadStream, createWriteStream, mkdirSync, readdirSync, statSync, unlinkSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { basename, dirname, join, relative, resolve, sep } from 'node:path'
import { ChunkMemo } from './dedup.js'
import {
    ChunkFetcher,
    DownloadFetchFn,
    JoinOptions,
    RecoverySource,
    collectManifestEntries,
    joinReference,
    makeChunkFetcher
} from './download.js'
import { getMaxShards, makeErasureBatch, makeIntermediateChunkHandler } from './erasure.js'
import { MantarayNode } from './manifest.js'
import { ChunkRef, EntryKind, FileRegistry } from './registry.js'
import { MigrationResult, SlotMap } from './slotmap.js'
import { makeEncryptedReplicas, makeReplicas } from './soc.js'
import { stamp } from './stamper.js'

const ENCODER = new TextEncoder()

type FetchFn = (
    url: string,
    init?: RequestInit
) => Promise<Pick<Response, 'ok' | 'status' | 'statusText'> & Partial<Pick<Response, 'text'>>>

export interface UploadOpts {
    signer: bigint
    batchId: Uint8Array
    uploadUrl: string
    batchDepth: number
    path: string
    stateDir: string
    encrypt?: boolean
    redundancyLevel?: number
    parallelism?: number
    fetchFn?: FetchFn
    onProgress?: (file: string, chunksProcessed: number) => void
}

export interface BenchSplitOpts {
    path: string
    encrypt?: boolean
    redundancyLevel?: number
    onProgress?: (file: string, chunksProcessed: number) => void
}

export interface BenchSignOpts {
    signer: bigint
    batchId: Uint8Array
    batchDepth: number
    path: string
    encrypt?: boolean
    redundancyLevel?: number
    onProgress?: (file: string, chunksProcessed: number) => void
}

export interface DeleteOpts {
    batchId: Uint8Array
    batchDepth: number
    rootHash: Uint8Array
    stateDir: string
}

export interface ListOpts {
    batchId: Uint8Array
    stateDir: string
}

export interface StatusOpts {
    batchId: Uint8Array
    batchDepth: number
    stateDir: string
}

export interface MigrateOpts {
    batchId: Uint8Array
    batchDepth: number
    stateDir: string
}

export interface ExportOpts {
    batchId: Uint8Array
    stateDir: string
    outDir: string
    downloadUrl: string
    parallelism?: number
    fetchFn?: DownloadFetchFn
    onProgress?: (reference: string, path: string, bytes: number) => void
}

export interface ExportedFile {
    path: string
    bytes: number
}

// How many chunks had to be rebuilt rather than simply downloaded. A successful export with a
// non-zero count means the content is intact but the upload has started to decay on the network.
export interface RecoveryCount {
    parity: number
    replica: number
}

export interface ExportResult {
    rootHash: Uint8Array
    // The local path the upload was made from, as recorded in the registry.
    source: string
    // Where the content was written: <outDir>/<reference>.
    directory: string
    files: ExportedFile[]
    recovered: RecoveryCount
    error?: string
}

function getPaths(stateDir: string, batchId: Uint8Array) {
    const prefix = Binary.uint8ArrayToHex(batchId).slice(0, 8)
    return {
        free: join(stateDir, `etherchunk-${prefix}.free`),
        idx: join(stateDir, `etherchunk-${prefix}.db`)
    }
}

const HEX_REFERENCE_PATTERN = /^[0-9a-fA-F]{64}$/

// A real Bee node answers a successful POST /chunks with 201 Created and a
// body like {"reference":"<64-char hex>"}. A misconfigured ETHERCHUNK_UPLOAD_URL
// (e.g. missing the /chunks suffix) hits Bee's root landing page instead,
// which happily returns 200 OK for any method — so response.ok alone can't
// catch it. Checking the actual response shape can.
async function validateChunkResponse(
    response: Pick<Response, 'status' | 'statusText'> & Partial<Pick<Response, 'text'>>
): Promise<string | null> {
    if (response.status !== 201) {
        return `expected 201 Created from POST /chunks, got ${response.status} ${response.statusText}`
    }
    if (!response.text) {
        return null
    }
    const bodyText = await response.text()
    let reference: unknown
    try {
        reference = JSON.parse(bodyText)?.reference
    } catch {
        return `expected a JSON {"reference": "<hex>"} body from POST /chunks, got: ${bodyText.slice(0, 200)}`
    }
    if (typeof reference !== 'string' || !HEX_REFERENCE_PATTERN.test(reference)) {
        return `expected a 64-char hex "reference" in the POST /chunks response, got: ${JSON.stringify(
            reference
        )} — check that ETHERCHUNK_UPLOAD_URL points at the Bee node's /chunks endpoint`
    }
    return null
}

export function buildChunkBody(chunk: Chunk, key?: Uint8Array): Uint8Array {
    if (key) {
        return Binary.concatBytes(
            Chunk.encryptSpan(key, Binary.numberToUint64(chunk.span, 'LE')),
            Chunk.encryptData(key, chunk.writer.buffer)
        )
    }
    return chunk.build()
}

interface UploadContext {
    signer: bigint
    batchId: Uint8Array
    uploadUrl: string
    fetchFn: FetchFn
    slotMap: SlotMap
    memo: ChunkMemo | null
    chunks: ChunkRef[]
    queue: AsyncQueue
    uploadErrors: Error[]
}

// Allocates a slot for one chunk address, stamps it and queues the POST. Takes the address
// and body directly so pre-built SOC replicas and split chunks share one path; `getData` is
// a thunk because a memo hit discards the body, and on the encrypted path building it means
// encrypting 4 KB for nothing.
function makeRawOnChunk(ctx: UploadContext): (address: Uint8Array, getData: () => Uint8Array) => Promise<void> {
    const { signer, batchId, uploadUrl, fetchFn, slotMap, memo, chunks, queue, uploadErrors } = ctx
    return async (address: Uint8Array, getData: () => Uint8Array) => {
        const bucket = Binary.uint16ToNumber(address, 'BE')
        // An address already stamped in this upload keeps its slot: same bytes, one stored
        // copy on Bee, N references from the tree above it. The ref is still pushed per
        // occurrence, so a deleted entry frees every slot it touched — freeSlot clears a
        // bit, so clearing a shared slot repeatedly is harmless.
        const memoizedSlot = memo?.get(address)
        if (memoizedSlot !== undefined) {
            chunks.push({ bucket, slot: memoizedSlot })
            return
        }
        const slot = slotMap.allocSlot(bucket)
        memo?.set(address, slot)
        chunks.push({ bucket, slot })
        const swarmPostageStamp = stamp(signer, batchId, address, slot)
        const body = getData()
        await queue.enqueue(async () => {
            try {
                const response = await fetchFn(uploadUrl, {
                    method: 'POST',
                    body: Buffer.from(body),
                    headers: { 'swarm-postage-stamp': swarmPostageStamp },
                    signal: AbortSignal.timeout(Dates.seconds(30))
                })
                const validationError = await validateChunkResponse(response)
                if (validationError) {
                    uploadErrors.push(new Error(`Failed to upload chunk: ${validationError}`))
                }
            } catch (err) {
                uploadErrors.push(err instanceof Error ? err : new Error(String(err)))
            }
        })
    }
}

function makeOnChunk(
    rawOnChunk: (address: Uint8Array, getData: () => Uint8Array) => Promise<void>
): (chunk: Chunk, key?: Uint8Array) => Promise<void> {
    return async (chunk: Chunk, key?: Uint8Array) => {
        const address = key ? chunk.encryptedHash(key).address : chunk.hash()
        await rawOnChunk(address, () => buildChunkBody(chunk, key))
    }
}

// Encryption derives a per-chunk key, so identical plaintext lands on distinct addresses
// and the memo could never hit — don't pay for the table on that path.
function makeChunkMemo(encrypt: boolean): ChunkMemo | null {
    return encrypt ? null : new ChunkMemo()
}

async function processPath(
    resolvedPath: string,
    encrypt: boolean,
    onChunk: (chunk: Chunk, key?: Uint8Array) => Promise<void>,
    setFile: (file: string) => void,
    redundancyLevel = 0,
    onReplica?: (address: Uint8Array, data: Uint8Array) => Promise<void>
): Promise<{ manifestRoot: Uint8Array; isDirectory: boolean }> {
    if (statSync(resolvedPath).isDirectory()) {
        // Pass 1: split all file content and collect hashes (32 or 64 bytes depending on encryption)
        const fileHashes = new Map<string, Uint8Array>()
        for (const filePath of walkDir(resolvedPath)) {
            const swarmPath = relative(resolvedPath, filePath)
            setFile(swarmPath)
            const { ref, rootChunk, encryptionKey } = await splitFile(filePath, onChunk, encrypt, redundancyLevel)
            fileHashes.set(swarmPath, ref)
            if (onReplica) {
                const replicas = encryptionKey
                    ? makeEncryptedReplicas(rootChunk, encryptionKey, redundancyLevel)
                    : makeReplicas(rootChunk, redundancyLevel)
                for (const replica of replicas) {
                    await onReplica(replica.address, replica.data)
                }
            }
        }

        // Pass 2: build the trie.
        // '/' is a standalone metadata fork at root key 47; file paths like 'index.html'
        // start at key 105 — completely different subtrees, no ordering conflict.
        const root = new MantarayNode({ encrypt })

        if (fileHashes.has('index.html')) {
            root.addFork(ENCODER.encode('/'), new Uint8Array(encrypt ? 64 : 32), {
                'website-index-document': 'index.html'
            })
        }

        for (const [swarmPath, hash] of fileHashes) {
            root.addFork(ENCODER.encode(swarmPath), hash, {
                'Content-Type': guessMimeType(swarmPath),
                Filename: basename(swarmPath)
            })
        }

        setFile('(manifest)')
        // The trie is saved with the same redundancy as the content below it: parity inside any node
        // wide enough to span several chunks, and replicas of every node's root chunk — including
        // the manifest root, which saveRecursively covers along with the rest.
        const { ref: manifestRoot } = await root.saveRecursively(onChunk, { redundancyLevel, onReplica })
        return { manifestRoot, isDirectory: true }
    } else {
        setFile(basename(resolvedPath))
        const {
            ref: fileRef,
            rootChunk,
            encryptionKey
        } = await splitFile(resolvedPath, onChunk, encrypt, redundancyLevel)
        if (onReplica) {
            const replicas = encryptionKey
                ? makeEncryptedReplicas(rootChunk, encryptionKey, redundancyLevel)
                : makeReplicas(rootChunk, redundancyLevel)
            for (const replica of replicas) {
                await onReplica(replica.address, replica.data)
            }
        }

        // Wrap in a manifest so the file is browseable via the Bzz gateway
        const filename = basename(resolvedPath)
        const root = new MantarayNode({ encrypt })
        setFile('(manifest)')
        root.addFork(ENCODER.encode('/'), new Uint8Array(encrypt ? 64 : 32), { 'website-index-document': filename })
        root.addFork(ENCODER.encode(filename), fileRef, {
            'Content-Type': guessMimeType(resolvedPath),
            Filename: filename
        })
        const { ref: manifestRoot } = await root.saveRecursively(onChunk, { redundancyLevel, onReplica })
        return { manifestRoot, isDirectory: false }
    }
}

export async function upload(opts: UploadOpts): Promise<Uint8Array> {
    const { signer, batchId, uploadUrl, batchDepth, stateDir } = opts
    const fetchFn: FetchFn = opts.fetchFn ?? fetch
    const resolvedPath = resolve(opts.path)

    mkdirSync(stateDir, { recursive: true })
    const { free, idx } = getPaths(stateDir, batchId)
    const slotMap = new SlotMap(free, batchDepth)
    const registry = new FileRegistry(idx)

    const encrypt = opts.encrypt ?? false
    const redundancyLevel = opts.redundancyLevel ?? 0

    const chunks: ChunkRef[] = []
    const uploadErrors: Error[] = []
    const parallelism = opts.parallelism ?? 32
    const queue = new AsyncQueue(parallelism, parallelism * 4)
    const rawOnAddress = makeRawOnChunk({
        signer,
        batchId,
        uploadUrl,
        fetchFn,
        slotMap,
        memo: makeChunkMemo(encrypt),
        chunks,
        queue,
        uploadErrors
    })
    const rawOnChunk = makeOnChunk(rawOnAddress)
    const rawOnReplica = async (address: Uint8Array, data: Uint8Array) => rawOnAddress(address, () => data)

    let chunksProcessed = 0
    let currentFile = ''
    const setFile = (file: string) => {
        currentFile = file
        chunksProcessed = 0
    }
    const onChunk = async (chunk: Chunk, key?: Uint8Array) => {
        await rawOnChunk(chunk, key)
        opts.onProgress?.(currentFile, ++chunksProcessed)
    }

    const { manifestRoot, isDirectory } = await processPath(
        resolvedPath,
        encrypt,
        onChunk,
        setFile,
        redundancyLevel,
        rawOnReplica
    )
    await queue.drain()
    if (uploadErrors.length > 0) throw uploadErrors[0]
    slotMap.save()
    registry.add(resolvedPath, manifestRoot, chunks, isDirectory ? 'manifest' : undefined, redundancyLevel)

    return manifestRoot
}

export async function benchSplit(opts: BenchSplitOpts): Promise<void> {
    const resolvedPath = resolve(opts.path)
    const encrypt = opts.encrypt ?? false
    const redundancyLevel = opts.redundancyLevel ?? 0

    let chunksProcessed = 0
    let currentFile = ''
    const setFile = (file: string) => {
        currentFile = file
        chunksProcessed = 0
    }
    const onChunk = async (_chunk: Chunk, _key?: Uint8Array) => {
        opts.onProgress?.(currentFile, ++chunksProcessed)
    }

    await processPath(resolvedPath, encrypt, onChunk, setFile, redundancyLevel)
}

export async function benchSign(opts: BenchSignOpts): Promise<void> {
    const { signer, batchId, batchDepth } = opts
    const resolvedPath = resolve(opts.path)
    const encrypt = opts.encrypt ?? false
    const redundancyLevel = opts.redundancyLevel ?? 0

    const tmpFree = join(tmpdir(), `etherchunk-bench-${process.pid}.free`)
    const slotMap = new SlotMap(tmpFree, batchDepth)
    // Mirrors upload()'s dedup so the benchmark reports the signing work a real upload
    // would do, and so a duplicate-heavy input doesn't exhaust a bucket here either.
    const memo = makeChunkMemo(encrypt)

    let chunksProcessed = 0
    let currentFile = ''
    const setFile = (file: string) => {
        currentFile = file
        chunksProcessed = 0
    }
    const onChunk = async (chunk: Chunk, key?: Uint8Array) => {
        const address = key ? chunk.encryptedHash(key).address : chunk.hash()
        if (memo?.get(address) === undefined) {
            const bucket = Binary.uint16ToNumber(address, 'BE')
            const slot = slotMap.allocSlot(bucket)
            memo?.set(address, slot)
            stamp(signer, batchId, address, slot)
        }
        opts.onProgress?.(currentFile, ++chunksProcessed)
    }

    try {
        await processPath(resolvedPath, encrypt, onChunk, setFile, redundancyLevel)
    } finally {
        try {
            unlinkSync(tmpFree)
        } catch {}
    }
}

export async function splitFile(
    filePath: string,
    onChunk: (chunk: Chunk, key?: Uint8Array) => Promise<void>,
    encrypt: boolean,
    redundancyLevel = 0
): Promise<{ ref: Uint8Array; rootChunk: Chunk; encryptionKey?: Uint8Array }> {
    const trackingOnChunk = async (chunk: Chunk, key?: Uint8Array) => {
        await onChunk(chunk, key)
    }
    const onBatch = makeErasureBatch(redundancyLevel, encrypt, trackingOnChunk)
    const splitter = new ChunkSplitter(
        onBatch,
        getMaxShards(redundancyLevel, encrypt),
        encrypt,
        makeIntermediateChunkHandler(redundancyLevel)
    )
    const readStream = createReadStream(filePath)
    for await (const bytes of readStream) {
        await splitter.append(bytes)
    }
    const rootChunk = await splitter.finalize()
    // 36.1.1: finalize() no longer calls onBatch for the root chunk — upload it explicitly.
    if (encrypt) {
        const { address, key: rootKey } = rootChunk.encryptedHash()
        await trackingOnChunk(rootChunk, rootKey)
        return { ref: Binary.concatBytes(address, rootKey), rootChunk, encryptionKey: rootKey }
    }
    await trackingOnChunk(rootChunk)
    return { ref: rootChunk.hash(), rootChunk }
}

export async function deleteFile(opts: DeleteOpts): Promise<void> {
    const { batchId, batchDepth, rootHash, stateDir } = opts
    const { free, idx } = getPaths(stateDir, batchId)
    const slotMap = new SlotMap(free, batchDepth)
    const registry = new FileRegistry(idx)

    const chunks = registry.removeByRootHash(rootHash)
    if (!chunks) {
        throw new Error(`File not found: ${Binary.uint8ArrayToHex(rootHash)}`)
    }

    for (const { bucket, slot } of chunks) {
        slotMap.freeSlot(bucket, slot)
    }
    slotMap.save()
}

export function list(opts: ListOpts): Array<{
    path: string
    rootHash: Uint8Array
    kind: EntryKind
    chunkCount: number
    redundancyLevel: number
    uploadDate: number | null
}> {
    const { idx } = getPaths(opts.stateDir, opts.batchId)
    return new FileRegistry(idx).list()
}

// Downloads every entry in this batch's registry back out of Swarm. Each upload lands in its own
// folder named after its Swarm reference, so re-exporting is idempotent and two entries can
// never write over each other. One unretrievable entry does not abort the rest — a batch can
// easily outlive the chunks of a single upload — so failures are reported per entry instead.
export async function exportAll(opts: ExportOpts): Promise<ExportResult[]> {
    // Like upload: opening the registry requires the directory to exist, and export can be the
    // first command someone runs against a batch on a fresh machine.
    mkdirSync(opts.stateDir, { recursive: true })
    const { idx } = getPaths(opts.stateDir, opts.batchId)
    const registry = new FileRegistry(idx)
    const fetcher = makeChunkFetcher(opts.downloadUrl, opts.fetchFn ?? fetch, opts.parallelism ?? 32)
    const outDir = resolve(opts.outDir)

    const results: ExportResult[] = []
    for (const entry of registry.list()) {
        const reference = Binary.uint8ArrayToHex(entry.rootHash)
        const directory = join(outDir, reference)
        const files: ExportedFile[] = []
        const recovered: RecoveryCount = { parity: 0, replica: 0 }
        // Dispersed replicas exist for the manifest root and for each file's root chunk, at the
        // level this upload was made with — the registry is the only record of that level, since
        // a root chunk carries no redundancy marker of its own.
        const joinOptions: JoinOptions = {
            replicaLevel: entry.redundancyLevel,
            onRecovered: (source: RecoverySource) => {
                recovered[source]++
            }
        }
        try {
            mkdirSync(directory, { recursive: true })
            const manifestEntries = await collectManifestEntries(fetcher, entry.rootHash, joinOptions)
            // A null result means the reference is not a manifest: uploads made before
            // etherchunk wrapped every file in one recorded the file's own root chunk, so the
            // reference is the content itself and the local file name is all there is to go on.
            const plan = manifestEntries ?? [
                { path: basename(entry.path), reference: entry.rootHash, metadata: null }
            ]
            for (const file of plan) {
                const path = file.path || basename(entry.path)
                const target = insideDirectory(directory, path)
                if (!target) {
                    throw new Error(`Manifest path escapes the export directory: ${path}`)
                }
                const bytes = await downloadToFile(fetcher, file.reference, target, joinOptions)
                files.push({ path, bytes })
                opts.onProgress?.(reference, path, bytes)
            }
            results.push({ rootHash: entry.rootHash, source: entry.path, directory, files, recovered })
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error)
            results.push({
                rootHash: entry.rootHash,
                source: entry.path,
                directory,
                files,
                recovered,
                error: message
            })
        }
    }
    return results
}

async function downloadToFile(
    fetcher: ChunkFetcher,
    reference: Uint8Array,
    target: string,
    options: JoinOptions
): Promise<number> {
    mkdirSync(dirname(target), { recursive: true })
    const stream = createWriteStream(target)
    // A write stream reports its failures as events, and an 'error' event with no listener takes
    // the whole process down. This one is attached for the stream's entire life, because the
    // window that matters is the one where nothing else is listening: a download that aborts
    // destroys the stream, and a stream destroyed before its file was even opened still reports
    // the failed open afterwards. Latching the error turns all of that into a return value.
    const failure: { error?: Error } = {}
    stream.on('error', error => {
        failure.error = error
    })
    try {
        const bytes = await joinReference(
            fetcher,
            reference,
            async data => {
                if (failure.error) {
                    throw failure.error
                }
                if (!stream.write(data)) {
                    await once(stream, 'drain')
                }
            },
            options
        )
        await new Promise<void>(resolve => stream.end(() => resolve()))
        if (failure.error) {
            throw failure.error
        }
        return bytes
    } catch (error) {
        stream.destroy()
        throw error
    }
}

// Manifest paths are data, not input: a path with '..' segments in a hand-built manifest would
// otherwise let an export write anywhere on disk.
function insideDirectory(directory: string, path: string): string | null {
    const root = resolve(directory)
    const target = resolve(root, path)
    return target.startsWith(root + sep) ? target : null
}

export function status(opts: StatusOpts) {
    const { free } = getPaths(opts.stateDir, opts.batchId)
    return new SlotMap(free, opts.batchDepth).getStats()
}

// Rewrites the .free file for this batch to match opts.batchDepth, preserving
// occupied-slot state. Safe to run whenever ETHERCHUNK_BATCH_DEPTH no longer matches
// the depth the local state was created with (e.g. after diluting the batch).
export function migrate(opts: MigrateOpts): MigrationResult {
    const { free } = getPaths(opts.stateDir, opts.batchId)
    return SlotMap.migrate(free, opts.batchDepth)
}

function walkDir(dir: string): string[] {
    const files: string[] = []
    for (const entry of readdirSync(dir, { withFileTypes: true, recursive: true })) {
        if (entry.isFile()) {
            files.push(join(entry.parentPath, entry.name))
        }
    }
    return files
}

function guessMimeType(filePath: string): string {
    const ext = filePath.slice(filePath.lastIndexOf('.')).toLowerCase()
    const types: Record<string, string> = {
        // HTML
        '.html': 'text/html',
        '.htm': 'text/html',

        // Styles
        '.css': 'text/css',

        // JavaScript / TypeScript
        '.js': 'application/javascript',
        '.mjs': 'application/javascript',
        '.cjs': 'application/javascript',
        '.tsx': 'text/typescript',
        '.jsx': 'text/jsx',

        // Data
        '.json': 'application/json',
        '.jsonld': 'application/ld+json',
        '.map': 'application/json',
        '.xml': 'application/xml',
        '.yaml': 'application/yaml',
        '.yml': 'application/yaml',
        '.toml': 'application/toml',
        '.csv': 'text/csv',
        '.txt': 'text/plain',
        '.md': 'text/markdown',

        // WebAssembly
        '.wasm': 'application/wasm',

        // Images
        '.png': 'image/png',
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.gif': 'image/gif',
        '.svg': 'image/svg+xml',
        '.ico': 'image/x-icon',
        '.webp': 'image/webp',
        '.avif': 'image/avif',
        '.apng': 'image/apng',
        '.bmp': 'image/bmp',
        '.tif': 'image/tiff',
        '.tiff': 'image/tiff',

        // Fonts
        '.woff': 'font/woff',
        '.woff2': 'font/woff2',
        '.ttf': 'font/ttf',
        '.otf': 'font/otf',
        '.eot': 'application/vnd.ms-fontobject',

        // Audio
        '.mp3': 'audio/mpeg',
        '.wav': 'audio/wav',
        '.ogg': 'audio/ogg',
        '.oga': 'audio/ogg',
        '.flac': 'audio/flac',
        '.aac': 'audio/aac',
        '.m4a': 'audio/mp4',

        // Video
        '.mp4': 'video/mp4',
        '.webm': 'video/webm',
        '.ogv': 'video/ogg',
        '.mov': 'video/quicktime',
        '.avi': 'video/x-msvideo',
        '.mkv': 'video/x-matroska',
        // .ts collides with TypeScript sources; HLS segments are the case that breaks
        // if served with the wrong type, so it wins here (matches nginx/Apache).
        '.ts': 'video/mp2t',

        // Documents
        '.pdf': 'application/pdf',
        '.rtf': 'application/rtf',
        '.doc': 'application/msword',
        '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        '.xls': 'application/vnd.ms-excel',
        '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        '.ppt': 'application/vnd.ms-powerpoint',
        '.pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',

        // Archives
        '.zip': 'application/zip',
        '.gz': 'application/gzip',
        '.tgz': 'application/gzip',
        '.tar': 'application/x-tar',
        '.bz2': 'application/x-bzip2',
        '.xz': 'application/x-xz',
        '.7z': 'application/x-7z-compressed',

        // Misc
        '.rss': 'application/rss+xml',
        '.atom': 'application/atom+xml',
        '.webmanifest': 'application/manifest+json'
    }
    return types[ext] ?? 'application/octet-stream'
}
