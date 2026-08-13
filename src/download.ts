import { AsyncQueue, Binary, Chunk, Dates } from 'cafe-utility'
import { getParities, rsReconstruct } from './erasure.js'
import { MantarayNode } from './manifest.js'
import { SOC_HEADER_SIZE, replicaAddresses } from './soc.js'

const DECODER = new TextDecoder()

const SPAN_SIZE = 8
const CHUNK_SIZE = 4096
const SEGMENT_SIZE = 32

// A Mantaray node marshals to a few KB at most: 64 bytes of header plus at most 256 forks of
// 32 header bytes, a reference and a little metadata. Anything bigger than this cannot be a
// manifest node, which is how a registry entry that points straight at file content is
// recognized without buffering the whole file to find out.
const MAX_MANIFEST_NODE_SIZE = 1024 * 1024

export type DownloadFetchFn = (
    url: string,
    init?: RequestInit
) => Promise<Pick<Response, 'ok' | 'status' | 'statusText' | 'arrayBuffer'>>

export type OnData = (data: Uint8Array) => Promise<void>

export interface ChunkFetcher {
    // Starts fetching these addresses so they are in flight before the walk needs them.
    prefetch(addresses: Uint8Array[]): Promise<void>
    // Resolves the chunk body (8-byte span + data), from the prefetch cache when possible.
    // Rejects when the chunk is unavailable or fails its address check.
    take(address: Uint8Array): Promise<Uint8Array>
    // A single fetch with no content-address check, for single-owner chunks — a SOC is addressed
    // by keccak256(id || owner) rather than by its content. Null instead of throwing, because
    // every caller is already on a recovery path where the next candidate is worth trying.
    fetchRaw(address: Uint8Array): Promise<Uint8Array | null>
}

export type RecoverySource = 'parity' | 'replica'

export interface JoinOptions {
    // Redundancy level recorded for the upload, which enables dispersed-replica recovery of the
    // reference itself — the one chunk in a tree that no parity covers. Reed-Solomon recovery
    // inside the tree needs no hint: every intermediate chunk states its own level in its span.
    replicaLevel?: number
    onRecovered?: (source: RecoverySource, address: Uint8Array) => void
}

export interface ManifestEntry {
    path: string
    reference: Uint8Array
    metadata: Record<string, string> | null
}

// Thrown by collectNode when the reference resolves to more data than a manifest node can
// hold — see MAX_MANIFEST_NODE_SIZE.
class NodeTooLargeError extends Error {}

// The chunk address is the BMT hash of the body exactly as it is stored — for an encrypted
// chunk that is the hash of the ciphertext — so a body can always be checked against the
// address it was fetched by.
export function chunkAddressFromBody(body: Uint8Array): Uint8Array {
    const chunk = new Chunk(Binary.uint64ToNumber(body.subarray(0, SPAN_SIZE), 'LE'))
    chunk.writer.buffer.set(body.subarray(SPAN_SIZE))
    return chunk.hash()
}

export function makeChunkFetcher(downloadUrl: string, fetchFn: DownloadFetchFn, parallelism: number): ChunkFetcher {
    const queue = new AsyncQueue(parallelism, parallelism * 4)
    const pending = new Map<string, Promise<Uint8Array>>()
    const base = downloadUrl.replace(/\/+$/, '')

    async function fetchChunk(hex: string, contentAddressed = true): Promise<Uint8Array> {
        const response = await fetchFn(`${base}/${hex}`, { signal: AbortSignal.timeout(Dates.seconds(30)) })
        if (!response.ok) {
            throw new Error(`Failed to download chunk ${hex}: ${response.status} ${response.statusText}`)
        }
        const body = new Uint8Array(await response.arrayBuffer())
        const maxLength = contentAddressed ? SPAN_SIZE + CHUNK_SIZE : SOC_HEADER_SIZE + SPAN_SIZE + CHUNK_SIZE
        if (body.length < SPAN_SIZE || body.length > maxLength) {
            throw new Error(`Chunk ${hex} came back as ${body.length} bytes, which is not a chunk`)
        }
        if (!contentAddressed) {
            return body
        }
        // Same reasoning as validateChunkResponse on the upload side: a URL that is not Bee's
        // /chunks endpoint still answers with something, and that something must never be
        // mistaken for chunk data — here it would be written into the exported file. It also
        // turns a corrupted chunk into a retrievable-but-unusable one, which the erasure path can
        // then repair exactly as if it had been missing.
        const address = Binary.uint8ArrayToHex(chunkAddressFromBody(body))
        if (address !== hex) {
            throw new Error(
                `Chunk ${hex} does not hash to its own address (got ${address}) — check that the download URL points at the Bee node's /chunks endpoint`
            )
        }
        return body
    }

    // Registers a fetch and returns once the queue has accepted it, so a caller that queues a
    // whole node's worth of children feels the queue's backpressure. AsyncQueue#enqueue throws
    // when a full queue already has a waiter, which is safe here because the tree walk is
    // strictly sequential: only one schedule() is ever in flight at a time.
    async function schedule(hex: string): Promise<void> {
        if (pending.has(hex)) {
            return
        }
        let settle!: (body: Uint8Array) => void
        let fail!: (error: unknown) => void
        const body = new Promise<Uint8Array>((resolve, reject) => {
            settle = resolve
            fail = reject
        })
        // Nothing awaits this promise until take() hands it out, and an unobserved rejection in
        // the meantime would crash the process.
        body.catch(() => {})
        pending.set(hex, body)
        await queue.enqueue(async () => {
            try {
                settle(await fetchChunk(hex))
            } catch (error) {
                fail(error)
            }
        })
    }

    return {
        prefetch: async (addresses: Uint8Array[]) => {
            for (const address of addresses) {
                await schedule(Binary.uint8ArrayToHex(address))
            }
        },
        take: async (address: Uint8Array) => {
            const hex = Binary.uint8ArrayToHex(address)
            await schedule(hex)
            const body = pending.get(hex)!
            // Every reference in a tree is taken once, so dropping the entry on the way out keeps
            // the prefetch cache from growing into a buffer of the whole file.
            pending.delete(hex)
            return body
        },
        fetchRaw: async (address: Uint8Array) => {
            try {
                return await fetchChunk(Binary.uint8ArrayToHex(address), false)
            } catch {
                return null
            }
        }
    }
}

// Downloads the chunk tree at `reference` (32 bytes, or 64 for an encrypted upload) and hands
// its content to onData in order. Returns the number of bytes emitted.
export async function joinReference(
    fetcher: ChunkFetcher,
    reference: Uint8Array,
    onData: OnData,
    options: JoinOptions = {}
): Promise<number> {
    const encrypted = reference.length === 64
    const address = reference.subarray(0, 32)
    const context: JoinContext = { fetcher, encrypted, onRecovered: options.onRecovered }
    const body = await takeOrRecover(context, address, options.replicaLevel ?? 0)
    const bytes = await joinNode(
        context,
        address,
        encrypted ? reference.subarray(32, 64) : undefined,
        body,
        onData
    )
    return Number(bytes)
}

// Nothing in the tree covers its own root: parity references live in the parent of the chunks
// they protect, and the root has no parent. Dispersed replicas are the only cover a root has,
// which is why they are worth a try before giving up on the whole upload.
async function takeOrRecover(context: JoinContext, address: Uint8Array, replicaLevel: number): Promise<Uint8Array> {
    try {
        return await context.fetcher.take(address)
    } catch (error) {
        const recovered = replicaLevel > 0 ? await recoverFromReplicas(context.fetcher, address, replicaLevel) : null
        if (!recovered) {
            throw error
        }
        context.onRecovered?.('replica', address)
        return recovered
    }
}

async function recoverFromReplicas(
    fetcher: ChunkFetcher,
    address: Uint8Array,
    replicaLevel: number
): Promise<Uint8Array | null> {
    const wanted = Binary.uint8ArrayToHex(address)
    for (const replicaAddress of replicaAddresses(address, replicaLevel)) {
        const soc = await fetcher.fetchRaw(replicaAddress)
        if (!soc || soc.length <= SOC_HEADER_SIZE) {
            continue
        }
        // The payload of a replica is the wrapped chunk body, span included. Checking that it
        // hashes to the address we wanted subsumes verifying the SOC signature: whoever wrote the
        // replica cannot produce content-addressed bytes they do not have.
        const payload = soc.subarray(SOC_HEADER_SIZE)
        if (Binary.uint8ArrayToHex(chunkAddressFromBody(payload)) === wanted) {
            return payload
        }
    }
    return null
}

// Walks the Mantaray trie at `root` and returns one entry per file it carries. Returns null
// when the reference is not a manifest at all: uploads made before etherchunk wrapped every
// file in a manifest recorded the file's own root chunk, and export still has to handle those.
export async function collectManifestEntries(
    fetcher: ChunkFetcher,
    root: Uint8Array,
    options: JoinOptions = {}
): Promise<ManifestEntry[] | null> {
    const rootData = await collectNode(fetcher, root, options)
    if (!rootData) {
        return null
    }
    let rootNode: MantarayNode
    try {
        rootNode = MantarayNode.unmarshalFromData(rootData)
    } catch {
        return null
    }
    const entries: ManifestEntry[] = []
    // Every node of the trie is saved as its own chunk tree, so every node has a root chunk of its
    // own that parity cannot reach — and therefore its own dispersed replicas, at the level the
    // upload was made with. The recursion carries the level for that reason.
    await walkNode(fetcher, rootNode, new Uint8Array(0), null, entries, options)
    return entries
}

async function walkNode(
    fetcher: ChunkFetcher,
    node: MantarayNode,
    prefix: Uint8Array,
    metadata: Record<string, string> | null,
    entries: ManifestEntry[],
    options: JoinOptions
): Promise<void> {
    // Only a node with a target reference carries content. The '/' fork that holds
    // website-index-document has an all-zero target and is skipped by this test — the file it
    // names is reached through its own fork anyway.
    if (!isAllZero(node.targetAddress)) {
        entries.push({ path: DECODER.decode(prefix), reference: node.targetAddress, metadata })
    }
    for (const fork of node.forks.values()) {
        const selfAddress = fork.node.selfAddress
        if (!selfAddress) {
            throw new Error('Manifest fork has no reference to its node')
        }
        const data = await collectNode(fetcher, selfAddress, options)
        if (!data) {
            throw new Error(`Manifest fork ${Binary.uint8ArrayToHex(selfAddress)} is not a Mantaray node`)
        }
        // Prefixes are concatenated as bytes, not as strings: a path is cut into 30-byte fork
        // prefixes, and the cut can land inside a multi-byte UTF-8 character.
        await walkNode(
            fetcher,
            MantarayNode.unmarshalFromData(data),
            Binary.concatBytes(prefix, fork.prefix),
            fork.node.metadata ?? null,
            entries,
            options
        )
    }
}

async function collectNode(
    fetcher: ChunkFetcher,
    reference: Uint8Array,
    options: JoinOptions
): Promise<Uint8Array | null> {
    const parts: Uint8Array[] = []
    let total = 0
    try {
        await joinReference(
            fetcher,
            reference,
            async data => {
                total += data.length
                if (total > MAX_MANIFEST_NODE_SIZE) {
                    throw new NodeTooLargeError()
                }
                parts.push(data)
            },
            options
        )
    } catch (error) {
        if (error instanceof NodeTooLargeError) {
            return null
        }
        throw error
    }
    return Binary.concatBytes(...parts)
}

interface JoinContext {
    fetcher: ChunkFetcher
    encrypted: boolean
    onRecovered?: (source: RecoverySource, address: Uint8Array) => void
}

// One chunk's worth of the tree. The body is passed in rather than fetched here because a missing
// chunk is rebuilt by its *parent*, from the parity references the parent holds — by the time a
// node is walked, its bytes have already been obtained one way or another.
async function joinNode(
    context: JoinContext,
    address: Uint8Array,
    key: Uint8Array | undefined,
    body: Uint8Array,
    onData: OnData
): Promise<bigint> {
    const { span, data } = key ? Chunk.decrypt(body, key) : readPlainChunk(body)
    const { size, level } = decodeSpan(span)

    if (size <= BigInt(CHUNK_SIZE)) {
        await onData(data.subarray(0, Number(size)))
        return size
    }

    const { data: dataRefs, parity: parityRefs } = splitReferences(data, level, context.encrypted)
    const children = await fetchChildren(context, dataRefs, parityRefs)

    let joined = 0n
    for (let i = 0; i < dataRefs.length && joined < size; i++) {
        const childAddress = dataRefs[i].subarray(0, 32)
        const childBody = children.bodies[i]
        if (!childBody) {
            throw new Error(
                `Chunk ${Binary.uint8ArrayToHex(childAddress)} could not be retrieved (${children.reasons.get(i)})` +
                    (parityRefs.length > 0 ? ' or reconstructed from parity' : '')
            )
        }
        // Release the body as it is consumed: a whole batch of children is held at once, and
        // without this every level of the tree would keep its batch alive to the very bottom.
        children.bodies[i] = null
        joined += await joinNode(
            context,
            childAddress,
            context.encrypted ? dataRefs[i].subarray(32, 64) : undefined,
            childBody,
            onData
        )
    }
    // The span of an intermediate chunk is the exact byte count of everything below it, so a
    // short join means part of the tree is gone from the network rather than merely slow.
    if (joined !== size) {
        throw new Error(
            `Chunk tree under ${Binary.uint8ArrayToHex(address)} is incomplete: expected ${size} bytes, joined ${joined}`
        )
    }
    return joined
}

interface Children {
    bodies: Array<Uint8Array | null>
    // Why a body is null, keyed by its index, so the failure can be reported with the reason the
    // fetch actually gave rather than a generic "missing".
    reasons: Map<number, string>
}

// Fetches every child of one intermediate chunk, then rebuilds whatever is missing from the
// parity references that live alongside them. An erasure batch is exactly the set of children of
// one node, which is what makes reconstruction a local operation.
async function fetchChildren(
    context: JoinContext,
    dataRefs: Uint8Array[],
    parityRefs: Uint8Array[]
): Promise<Children> {
    const addresses = dataRefs.map(reference => reference.subarray(0, 32))
    await context.fetcher.prefetch(addresses)

    const bodies: Array<Uint8Array | null> = []
    const reasons = new Map<number, string>()
    for (let i = 0; i < addresses.length; i++) {
        try {
            bodies.push(await context.fetcher.take(addresses[i]))
        } catch (error) {
            bodies.push(null)
            reasons.set(i, error instanceof Error ? error.message : String(error))
        }
    }

    const missing = [...reasons.keys()]
    if (missing.length === 0 || missing.length > parityRefs.length) {
        return { bodies, reasons }
    }

    const parityAddresses = parityRefs.map(reference => reference.subarray(0, 32))
    await context.fetcher.prefetch(parityAddresses)
    const shards: Array<Uint8Array | null> = bodies.slice()
    for (const parityAddress of parityAddresses) {
        try {
            shards.push(await context.fetcher.take(parityAddress))
        } catch {
            shards.push(null)
        }
    }

    const rebuilt = rsReconstruct(shards, dataRefs.length, parityRefs.length)
    if (!rebuilt) {
        return { bodies, reasons }
    }
    for (const index of missing) {
        const candidate = rebuilt[index]
        // Reconstruction is only trusted once the result hashes to the address that was wanted.
        // Nothing else here is checkable — a wrong data/parity split or a bad shard would produce
        // plausible-looking bytes — and this is what keeps repaired content honest.
        if (candidate && Binary.uint8ArrayToHex(chunkAddressFromBody(candidate)) === Binary.uint8ArrayToHex(addresses[index])) {
            bodies[index] = candidate
            reasons.delete(index)
            context.onRecovered?.('parity', addresses[index])
        }
    }
    return { bodies, reasons }
}

function readPlainChunk(body: Uint8Array): { span: bigint; data: Uint8Array } {
    return { span: Binary.uint64ToNumber(body.subarray(0, SPAN_SIZE), 'LE'), data: body.subarray(SPAN_SIZE) }
}

// Bee stores the redundancy level of an erasure-coded intermediate chunk in the top byte of its
// span (redundancy.Encode: span[7] = level | 0x80), which both flags "parity references follow
// the data references in this chunk" and overwrites the high byte of the real span. Spans that
// large need a 56-bit file, so masking the byte off recovers the byte count.
function decodeSpan(span: bigint): { size: bigint; level: number } {
    const top = Number((span >> 56n) & 0xffn)
    if ((top & 0x80) === 0) {
        return { size: span, level: 0 }
    }
    return { size: span & 0x00ffffffffffffffn, level: top & 0x7f }
}

// Splits an intermediate chunk's references into the ones that carry its content and the parity
// references an erasure-coded upload appends after them. Parity references are 32 bytes wide even
// on the encrypted path — parity chunks are plain chunks computed over ciphertext, which is why
// Bee budgets encrypted shards as (128 - parities) / 2 — so the boundary is found by solving for
// the shard count that reproduces the number of 32-byte segments actually written.
//
// The solve has exactly one answer: shards * segmentsPerRef + parities(shards) is strictly
// increasing in shards for every level, in both modes. Reconstruction depends on that (the shard
// count sets up the decode matrix), where the walk itself does not — span accounting stops it
// regardless.
function splitReferences(
    data: Uint8Array,
    level: number,
    encrypted: boolean
): { data: Uint8Array[]; parity: Uint8Array[] } {
    const refSize = encrypted ? 64 : 32
    const segmentsPerRef = refSize / SEGMENT_SIZE
    const segments = usedSegments(data)
    let count = Math.floor(segments / segmentsPerRef)
    let parityCount = 0
    if (level > 0) {
        for (let shards = 1; shards * segmentsPerRef <= segments; shards++) {
            const parities = getParities(level, shards, encrypted)
            if (shards * segmentsPerRef + parities === segments) {
                count = shards
                parityCount = parities
                break
            }
        }
    }
    const dataRefs: Uint8Array[] = []
    for (let i = 0; i < count; i++) {
        dataRefs.push(data.subarray(i * refSize, (i + 1) * refSize))
    }
    const parityRefs: Uint8Array[] = []
    const parityStart = count * refSize
    for (let i = 0; i < parityCount; i++) {
        parityRefs.push(data.subarray(parityStart + i * SEGMENT_SIZE, parityStart + (i + 1) * SEGMENT_SIZE))
    }
    return { data: dataRefs, parity: parityRefs }
}

// How many 32-byte segments of an intermediate chunk were written to. The trailing segments are
// the zero padding of the 4 KB buffer; a written segment is half of a reference, so it is either
// an address or an encryption key and never all zeros.
function usedSegments(data: Uint8Array): number {
    let segments = Math.floor(data.length / SEGMENT_SIZE)
    while (segments > 0 && isAllZero(data.subarray((segments - 1) * SEGMENT_SIZE, segments * SEGMENT_SIZE))) {
        segments--
    }
    return segments
}

function isAllZero(bytes: Uint8Array): boolean {
    return bytes.every(byte => byte === 0)
}
