import { AsyncQueue, Binary, Chunk, ChunkSplitter, Dates } from 'cafe-utility'
import { createReadStream, mkdirSync, readdirSync, statSync, unlinkSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { basename, join, relative, resolve } from 'node:path'
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
        return `expected a 64-char hex "reference" in the POST /chunks response, got: ${JSON.stringify(reference)} — check that ETHERCHUNK_UPLOAD_URL points at the Bee node's /chunks endpoint`
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

function makeOnChunk(
    signer: bigint,
    batchId: Uint8Array,
    uploadUrl: string,
    fetchFn: FetchFn,
    slotMap: SlotMap,
    chunks: ChunkRef[],
    queue: AsyncQueue,
    uploadErrors: Error[]
): (chunk: Chunk, key?: Uint8Array) => Promise<void> {
    return async (chunk: Chunk, key?: Uint8Array) => {
        const address = key ? chunk.encryptedHash(key).address : chunk.hash()
        const bucket = Binary.uint16ToNumber(address, 'BE')
        const slot = slotMap.allocSlot(bucket)
        chunks.push({ bucket, slot })
        const swarmPostageStamp = stamp(signer, batchId, address, slot)
        const body = buildChunkBody(chunk, key)
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

// Like makeOnChunk but for pre-built SOC replicas where address and body are already known.
// Shares the same slot allocator, upload queue, and error collector as regular chunks.
function makeRawOnChunk(
    signer: bigint,
    batchId: Uint8Array,
    uploadUrl: string,
    fetchFn: FetchFn,
    slotMap: SlotMap,
    chunks: ChunkRef[],
    queue: AsyncQueue,
    uploadErrors: Error[]
): (address: Uint8Array, data: Uint8Array) => Promise<void> {
    return async (address: Uint8Array, data: Uint8Array) => {
        const bucket = Binary.uint16ToNumber(address, 'BE')
        const slot = slotMap.allocSlot(bucket)
        chunks.push({ bucket, slot })
        const swarmPostageStamp = stamp(signer, batchId, address, slot)
        await queue.enqueue(async () => {
            try {
                const response = await fetchFn(uploadUrl, {
                    method: 'POST',
                    body: Buffer.from(data),
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
        const {
            ref: manifestRoot,
            rootChunk: manifestRootChunk,
            encryptionKey: manifestKey
        } = await root.saveRecursively(onChunk)
        if (onReplica) {
            const replicas = manifestKey
                ? makeEncryptedReplicas(manifestRootChunk, manifestKey, redundancyLevel)
                : makeReplicas(manifestRootChunk, redundancyLevel)
            for (const replica of replicas) {
                await onReplica(replica.address, replica.data)
            }
        }
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
        const {
            ref: manifestRoot,
            rootChunk: manifestRootChunk,
            encryptionKey: manifestKey
        } = await root.saveRecursively(onChunk)
        if (onReplica) {
            const replicas = manifestKey
                ? makeEncryptedReplicas(manifestRootChunk, manifestKey, redundancyLevel)
                : makeReplicas(manifestRootChunk, redundancyLevel)
            for (const replica of replicas) {
                await onReplica(replica.address, replica.data)
            }
        }
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

    const chunks: ChunkRef[] = []
    const uploadErrors: Error[] = []
    const parallelism = opts.parallelism ?? 32
    const queue = new AsyncQueue(parallelism, parallelism * 4)
    const rawOnChunk = makeOnChunk(signer, batchId, uploadUrl, fetchFn, slotMap, chunks, queue, uploadErrors)
    const rawOnReplica = makeRawOnChunk(signer, batchId, uploadUrl, fetchFn, slotMap, chunks, queue, uploadErrors)

    const encrypt = opts.encrypt ?? false
    const redundancyLevel = opts.redundancyLevel ?? 0

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

    let chunksProcessed = 0
    let currentFile = ''
    const setFile = (file: string) => {
        currentFile = file
        chunksProcessed = 0
    }
    const onChunk = async (chunk: Chunk, key?: Uint8Array) => {
        const address = key ? chunk.encryptedHash(key).address : chunk.hash()
        const bucket = Binary.uint16ToNumber(address, 'BE')
        const slot = slotMap.allocSlot(bucket)
        stamp(signer, batchId, address, slot)
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
        '.html': 'text/html',
        '.htm': 'text/html',
        '.css': 'text/css',
        '.js': 'application/javascript',
        '.mjs': 'application/javascript',
        '.json': 'application/json',
        '.png': 'image/png',
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.gif': 'image/gif',
        '.svg': 'image/svg+xml',
        '.ico': 'image/x-icon',
        '.webp': 'image/webp',
        '.woff': 'font/woff',
        '.woff2': 'font/woff2',
        '.ttf': 'font/ttf',
        '.txt': 'text/plain',
        '.xml': 'application/xml',
        '.pdf': 'application/pdf',
        '.mp4': 'video/mp4',
        '.webm': 'video/webm',
        '.mp3': 'audio/mpeg',
        '.wav': 'audio/wav'
    }
    return types[ext] ?? 'application/octet-stream'
}
