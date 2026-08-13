import { Binary, Chunk, ChunkSplitter, Uint8ArrayReader } from 'cafe-utility'
import { randomBytes } from 'node:crypto'
import { getMaxShards, makeErasureBatch, makeIntermediateChunkHandler } from './erasure.js'
import { makeEncryptedReplicas, makeReplicas } from './soc.js'

const ENCODER = new TextEncoder()
const DECODER = new TextDecoder()

const TYPE_VALUE = 2
const TYPE_EDGE = 4
const TYPE_WITH_PATH_SEPARATOR = 8
const TYPE_WITH_METADATA = 16
const PATH_SEPARATOR = new Uint8Array([47])
const VERSION_02_HASH_HEX = '5768b3b6a7db56d21d1abff40d41cebfc83448fed8d7e9b06ec0d3b073f28f7b'
const VERSION_02_HASH = Binary.hexToUint8Array(VERSION_02_HASH_HEX)

function isType(value: number, type: number): boolean {
    return (value & type) === type
}

export class Fork {
    prefix: Uint8Array
    node: MantarayNode

    constructor(prefix: Uint8Array, node: MantarayNode) {
        this.prefix = prefix
        this.node = node
    }

    static split(a: Fork, b: Fork): Fork {
        const commonPart = Binary.commonPrefix(a.prefix, b.prefix)

        if (commonPart.length === a.prefix.length) {
            const remainingB = b.prefix.slice(commonPart.length)
            b.node.path = b.prefix.slice(commonPart.length)
            b.prefix = b.prefix.slice(commonPart.length)
            b.node.parent = a.node
            a.node.forks.set(remainingB[0], b)

            return a
        }

        if (commonPart.length === b.prefix.length) {
            const remainingA = a.prefix.slice(commonPart.length)
            a.node.path = a.prefix.slice(commonPart.length)
            a.prefix = a.prefix.slice(commonPart.length)
            a.node.parent = b.node
            b.node.forks.set(remainingA[0], a)

            return b
        }

        const node = new MantarayNode({ path: commonPart, encrypt: a.node.encrypt })

        const newAFork = new Fork(a.prefix.slice(commonPart.length), a.node)
        const newBFork = new Fork(b.prefix.slice(commonPart.length), b.node)

        a.node.path = a.prefix.slice(commonPart.length)
        b.node.path = b.prefix.slice(commonPart.length)
        a.prefix = a.prefix.slice(commonPart.length)
        b.prefix = b.prefix.slice(commonPart.length)

        node.forks.set(newAFork.prefix[0], newAFork)
        node.forks.set(newBFork.prefix[0], newBFork)

        newAFork.node.parent = node
        newBFork.node.parent = node

        return new Fork(commonPart, node)
    }

    marshal(): Uint8Array {
        if (!this.node.selfAddress) {
            throw Error('Fork#marshal node.selfAddress is not set')
        }
        const data: Uint8Array[] = []
        data.push(new Uint8Array([this.node.determineType()]))
        data.push(Binary.numberToUint8(this.prefix.length))
        data.push(this.prefix)

        if (this.prefix.length < 30) {
            data.push(new Uint8Array(30 - this.prefix.length))
        }
        data.push(this.node.selfAddress)

        if (this.node.metadata) {
            const metadataBytes = Binary.padEndToMultiple(
                new Uint8Array([0x00, 0x00, ...ENCODER.encode(JSON.stringify(this.node.metadata))]),
                32,
                0x0a
            )
            const metadataLengthBytes = Binary.numberToUint16(metadataBytes.length - 2, 'BE')
            metadataBytes.set(metadataLengthBytes, 0)
            data.push(metadataBytes)
        }

        return Binary.concatBytes(...data)
    }

    static unmarshal(reader: Uint8ArrayReader, addressLength: number): Fork {
        const type = Binary.uint8ToNumber(reader.read(1))
        const prefixLength = Binary.uint8ToNumber(reader.read(1))
        const prefix = reader.read(prefixLength)
        if (prefixLength < 30) {
            reader.read(30 - prefixLength)
        }
        const selfAddress = reader.read(addressLength)
        let metadata: Record<string, string> | undefined = undefined
        if (isType(type, TYPE_WITH_METADATA)) {
            const metadataLength = Binary.uint16ToNumber(reader.read(2), 'BE')
            if (metadataLength > reader.max()) {
                throw new Error('Fork#unmarshal not enough bytes for metadata')
            }
            metadata = JSON.parse(DECODER.decode(reader.read(metadataLength)))
        }
        return new Fork(prefix, new MantarayNode({ selfAddress, metadata, path: prefix }))
    }
}

export interface SaveOptions {
    // Redundancy level of the upload the manifest belongs to. Applies to the trie exactly as it
    // applies to file content: parity chunks inside every node that is more than one chunk wide,
    // dispersed replicas for every node's root chunk.
    redundancyLevel?: number
    onReplica?: (address: Uint8Array, data: Uint8Array) => Promise<void>
}

// Each node in the trie is saved through its own splitter, so each one ends in a root chunk that no
// parity can cover — the same position a file root is in, and the reason dispersed replicas exist.
// Bee replicates every root its hash-trie writer produces (hashtrie.Sum puts replicas whenever the
// level is not NONE), and a manifest node save is one such pipeline run, so replicating interior
// nodes rather than only the manifest root is what matches it.
async function saveReplicas(
    rootChunk: Chunk,
    level: number,
    onReplica?: (address: Uint8Array, data: Uint8Array) => Promise<void>,
    key?: Uint8Array
): Promise<void> {
    if (!onReplica || level === 0) {
        return
    }
    const replicas = key ? makeEncryptedReplicas(rootChunk, key, level) : makeReplicas(rootChunk, level)
    for (const replica of replicas) {
        await onReplica(replica.address, replica.data)
    }
}

interface MantarayNodeOptions {
    selfAddress?: Uint8Array
    targetAddress?: Uint8Array
    obfuscationKey?: Uint8Array
    metadata?: Record<string, string> | null
    path?: Uint8Array | null
    parent?: MantarayNode | null
    encrypt?: boolean
}

export class MantarayNode {
    public obfuscationKey: Uint8Array = new Uint8Array(32)
    public selfAddress: Uint8Array | null = null
    public targetAddress: Uint8Array = new Uint8Array(32)
    public metadata: Record<string, string> | undefined | null = null
    public path: Uint8Array = new Uint8Array(0)
    public forks: Map<number, Fork> = new Map()
    public parent: MantarayNode | null = null
    public encrypt: boolean = false

    constructor(options?: MantarayNodeOptions) {
        if (options?.encrypt) {
            this.encrypt = true
        }

        if (options?.targetAddress) {
            this.targetAddress = options.targetAddress
        } else if (this.encrypt) {
            this.targetAddress = new Uint8Array(64)
        }

        if (options?.selfAddress) {
            this.selfAddress = options.selfAddress
        }

        if (options?.metadata) {
            this.metadata = options.metadata
        }

        if (options?.obfuscationKey) {
            this.obfuscationKey = options.obfuscationKey
        }

        if (options?.path) {
            this.path = options.path
        }

        if (options?.parent) {
            this.parent = options.parent
        }
    }

    get fullPath(): Uint8Array {
        return Binary.concatBytes(this.parent?.fullPath ?? new Uint8Array(0), this.path)
    }

    get fullPathString(): string {
        return DECODER.decode(this.fullPath)
    }

    /**
     * Gets the binary representation of the node.
     */
    async marshal(): Promise<Uint8Array> {
        for (const fork of this.forks.values()) {
            if (!fork.node.selfAddress) {
                fork.node.selfAddress = await fork.node.calculateSelfAddress()
            }
        }
        // The obfuscation key randomizes every manifest node, encrypted or not: it is XORed
        // over the serialized body and stored in the clear as the node's first 32 bytes, so
        // readers recover the content either way. Leaving it all-zeros makes structurally
        // identical nodes byte-identical — and therefore address-identical. The '/' metadata
        // fork node (target address all-zeros, no forks of its own, metadata held by the
        // parent) serializes the same for *every* upload, so a zero key made every upload
        // re-stamp one fixed chunk, piling slots into a single postage bucket until that
        // bucket filled while the batch was barely used. A fresh key per node scatters them.
        if (Binary.equals(this.obfuscationKey, new Uint8Array(32))) {
            this.obfuscationKey = new Uint8Array(randomBytes(32))
        }
        const header = new Uint8Array(32)
        header.set(VERSION_02_HASH, 0)
        header.set(Binary.numberToUint8(this.targetAddress.length), 31)
        const forkBitmap = new Uint8Array(32)
        for (const fork of this.forks.keys()) {
            Binary.setBit(forkBitmap, fork, 1, 'LE')
        }
        const forks: Uint8Array[] = []
        for (let i = 0; i < 256; i++) {
            if (Binary.getBit(forkBitmap, i, 'LE')) {
                forks.push(this.forks.get(i)!.marshal())
            }
        }
        const data = Binary.xorCypher(
            Binary.concatBytes(header, this.targetAddress, forkBitmap, ...forks),
            this.obfuscationKey
        )

        return Binary.concatBytes(this.obfuscationKey, data)
    }

    /**
     * Adds a fork to the node.
     */
    addFork(path: Uint8Array, reference: Uint8Array, metadata?: Record<string, string> | null) {
        this.selfAddress = null
        let tip: MantarayNode = this
        while (path.length) {
            const prefix = path.slice(0, 30)
            path = path.slice(30)
            const isLast = path.length === 0

            const [bestMatch, matchedPath] = tip.findClosest(prefix)
            const remainingPath = prefix.slice(matchedPath.length)

            if (matchedPath.length) {
                tip = bestMatch
            }

            if (!remainingPath.length) {
                continue
            }

            const newFork = new Fork(
                remainingPath,
                new MantarayNode({
                    targetAddress: isLast ? reference : undefined,
                    metadata: isLast ? metadata : undefined,
                    path: remainingPath,
                    encrypt: this.encrypt
                })
            )

            const existing = bestMatch.forks.get(remainingPath[0])

            if (existing) {
                const fork = Fork.split(newFork, existing)
                tip.forks.set(remainingPath[0], fork)
                fork.node.parent = tip
                tip.selfAddress = null
                tip = newFork.node
            } else {
                tip.forks.set(remainingPath[0], newFork)
                newFork.node.parent = tip
                tip.selfAddress = null
                tip = newFork.node
            }
        }
    }

    /**
     * Calculates the self address of the node.
     */
    async calculateSelfAddress(): Promise<Uint8Array> {
        if (this.selfAddress) {
            return this.selfAddress
        }
        if (this.encrypt) {
            throw new Error('calculateSelfAddress is not supported for encrypted nodes — use saveRecursively')
        }
        return (await ChunkSplitter.root(await this.marshal())).hash()
    }

    /**
     * Saves the node and its children recursively.
     * Returns the 32-byte manifest root hash (or 64-byte encrypted ref) and
     * the root Chunk so callers can create dispersed replicas.
     */
    async saveRecursively(
        onChunk: (chunk: Chunk, key?: Uint8Array) => Promise<void>,
        options: SaveOptions = {}
    ): Promise<{ ref: Uint8Array; rootChunk: Chunk; encryptionKey?: Uint8Array }> {
        for (const fork of this.forks.values()) {
            await fork.node.saveRecursively(onChunk, options)
        }
        return this.saveNode(onChunk, options)
    }

    private async saveNode(
        onChunk: (chunk: Chunk, key?: Uint8Array) => Promise<void>,
        options: SaveOptions
    ): Promise<{ ref: Uint8Array; rootChunk: Chunk; encryptionKey?: Uint8Array }> {
        const level = options.redundancyLevel ?? 0
        // A manifest node is split by the same erasure-aware pipeline as file content, because Bee
        // saves every Mantaray node through the redundancy-configured pipeline: api/dirs.go hands
        // requestPipelineFactory(ctx, putter, encrypt, rLevel) to the loadsave that manifest writes
        // go through. So a node wide enough to span several chunks carries parity references in its
        // intermediate chunks, exactly as a file's tree does — without this a --redundancy upload
        // had protected content under an unprotected trie.
        const splitter = new ChunkSplitter(
            makeErasureBatch(level, this.encrypt, onChunk),
            getMaxShards(level, this.encrypt),
            this.encrypt,
            makeIntermediateChunkHandler(level)
        )
        await splitter.append(await this.marshal())
        const rootChunk = await splitter.finalize()
        // 36.1.1: finalize() no longer calls onBatch for the root chunk — upload it explicitly.
        if (this.encrypt) {
            const { address, key: rootKey } = rootChunk.encryptedHash()
            await onChunk(rootChunk, rootKey)
            this.selfAddress = Binary.concatBytes(address, rootKey)
            await saveReplicas(rootChunk, level, options.onReplica, rootKey)
            return { ref: this.selfAddress, rootChunk, encryptionKey: rootKey }
        }
        await onChunk(rootChunk)
        this.selfAddress = rootChunk.hash()
        await saveReplicas(rootChunk, level, options.onReplica)
        return { ref: this.selfAddress, rootChunk }
    }

    static unmarshalFromData(data: Uint8Array, selfAddress?: Uint8Array): MantarayNode {
        if (data.length < 64) {
            throw new Error('MantarayNode#unmarshalFromData data too short')
        }
        const obfuscationKey = data.subarray(0, 32)
        const decrypted = Binary.xorCypher(data.subarray(32), obfuscationKey)
        const reader = new Uint8ArrayReader(decrypted)
        const versionHash = reader.read(31)
        if (!Binary.equals(versionHash, VERSION_02_HASH.slice(0, 31))) {
            throw new Error('MantarayNode#unmarshalFromData invalid version hash')
        }
        const refBytesSize = Binary.uint8ToNumber(reader.read(1))
        if (refBytesSize === 0) {
            throw new Error('MantarayNode#unmarshalFromData refBytesSize is 0')
        }
        const targetAddress = reader.read(refBytesSize)
        const node = new MantarayNode({ selfAddress, targetAddress, obfuscationKey })
        const forkBitmap = reader.read(32)
        for (let i = 0; i < 256; i++) {
            if (Binary.getBit(forkBitmap, i, 'LE')) {
                const fork = Fork.unmarshal(reader, refBytesSize)
                node.forks.set(i, fork)
                fork.node.parent = node
            }
        }
        return node
    }

    /**
     * Finds a node in the tree by its path.
     */
    find(path: string | Uint8Array): MantarayNode | null {
        const [closest, match] = this.findClosest(path)

        return match.length === path.length ? closest : null
    }

    /**
     * Finds the closest node in the tree to the given path.
     */
    findClosest(path: string | Uint8Array, current: Uint8Array = new Uint8Array()): [MantarayNode, Uint8Array] {
        path = path instanceof Uint8Array ? path : ENCODER.encode(path)

        if (path.length === 0) {
            return [this, current]
        }

        const fork = this.forks.get(path[0])

        if (fork && Binary.commonPrefix(fork.prefix, path).length === fork.prefix.length) {
            return fork.node.findClosest(path.slice(fork.prefix.length), Binary.concatBytes(current, fork.prefix))
        }

        return [this, current]
    }

    determineType() {
        let type = 0

        const nullAddress = new Uint8Array(this.targetAddress.length)
        if (!Binary.equals(this.targetAddress, nullAddress) || Binary.equals(this.path, PATH_SEPARATOR)) {
            type |= TYPE_VALUE
        }

        if (this.forks.size > 0) {
            type |= TYPE_EDGE
        }

        if (Binary.indexOf(this.path, PATH_SEPARATOR) !== -1 && !Binary.equals(this.path, PATH_SEPARATOR)) {
            type |= TYPE_WITH_PATH_SEPARATOR
        }

        if (this.metadata) {
            type |= TYPE_WITH_METADATA
        }

        return type
    }
}
