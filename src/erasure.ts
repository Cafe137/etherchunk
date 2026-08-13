import { Binary, Chunk, ChunkEntry } from 'cafe-utility'

// GF(2^8) logarithm table — polynomial 29, matches klauspost/reedsolomon
// logTable[0] is unused (0 has no logarithm in the multiplicative group)
const LOG_TABLE = new Uint8Array([
    0, 0, 1, 25, 2, 50, 26, 198, 3, 223, 51, 238, 27, 104, 199, 75, 4, 100, 224, 14, 52, 141, 239, 129, 28, 193, 105,
    248, 200, 8, 76, 113, 5, 138, 101, 47, 225, 36, 15, 33, 53, 147, 142, 218, 240, 18, 130, 69, 29, 181, 194, 125, 106,
    39, 249, 185, 201, 154, 9, 120, 77, 228, 114, 166, 6, 191, 139, 98, 102, 221, 48, 253, 226, 152, 37, 179, 16, 145,
    34, 136, 54, 208, 148, 206, 143, 150, 219, 189, 241, 210, 19, 92, 131, 56, 70, 64, 30, 66, 182, 163, 195, 72, 126,
    110, 107, 58, 40, 84, 250, 133, 186, 61, 202, 94, 155, 159, 10, 21, 121, 43, 78, 212, 229, 172, 115, 243, 167, 87,
    7, 112, 192, 247, 140, 128, 99, 13, 103, 74, 222, 237, 49, 197, 254, 24, 227, 165, 153, 119, 38, 184, 180, 124, 17,
    68, 146, 217, 35, 32, 137, 46, 55, 63, 209, 91, 149, 188, 207, 205, 144, 135, 151, 178, 220, 252, 190, 97, 242, 86,
    211, 171, 20, 42, 93, 158, 132, 60, 57, 83, 71, 109, 65, 162, 31, 45, 67, 216, 183, 123, 164, 118, 196, 23, 73, 236,
    127, 12, 111, 246, 108, 161, 59, 82, 41, 157, 85, 170, 251, 96, 134, 177, 187, 204, 62, 90, 203, 89, 95, 176, 156,
    169, 160, 81, 11, 245, 22, 235, 122, 117, 44, 215, 79, 174, 213, 233, 230, 231, 173, 232, 116, 214, 244, 234, 168,
    80, 88, 175
])

// GF(2^8) exponentiation table — first 255 entries (cyclic, index via % 255)
const EXP_TABLE = new Uint8Array([
    0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80, 0x1d, 0x3a, 0x74, 0xe8, 0xcd, 0x87, 0x13, 0x26, 0x4c, 0x98, 0x2d,
    0x5a, 0xb4, 0x75, 0xea, 0xc9, 0x8f, 0x03, 0x06, 0x0c, 0x18, 0x30, 0x60, 0xc0, 0x9d, 0x27, 0x4e, 0x9c, 0x25, 0x4a,
    0x94, 0x35, 0x6a, 0xd4, 0xb5, 0x77, 0xee, 0xc1, 0x9f, 0x23, 0x46, 0x8c, 0x05, 0x0a, 0x14, 0x28, 0x50, 0xa0, 0x5d,
    0xba, 0x69, 0xd2, 0xb9, 0x6f, 0xde, 0xa1, 0x5f, 0xbe, 0x61, 0xc2, 0x99, 0x2f, 0x5e, 0xbc, 0x65, 0xca, 0x89, 0x0f,
    0x1e, 0x3c, 0x78, 0xf0, 0xfd, 0xe7, 0xd3, 0xbb, 0x6b, 0xd6, 0xb1, 0x7f, 0xfe, 0xe1, 0xdf, 0xa3, 0x5b, 0xb6, 0x71,
    0xe2, 0xd9, 0xaf, 0x43, 0x86, 0x11, 0x22, 0x44, 0x88, 0x0d, 0x1a, 0x34, 0x68, 0xd0, 0xbd, 0x67, 0xce, 0x81, 0x1f,
    0x3e, 0x7c, 0xf8, 0xed, 0xc7, 0x93, 0x3b, 0x76, 0xec, 0xc5, 0x97, 0x33, 0x66, 0xcc, 0x85, 0x17, 0x2e, 0x5c, 0xb8,
    0x6d, 0xda, 0xa9, 0x4f, 0x9e, 0x21, 0x42, 0x84, 0x15, 0x2a, 0x54, 0xa8, 0x4d, 0x9a, 0x29, 0x52, 0xa4, 0x55, 0xaa,
    0x49, 0x92, 0x39, 0x72, 0xe4, 0xd5, 0xb7, 0x73, 0xe6, 0xd1, 0xbf, 0x63, 0xc6, 0x91, 0x3f, 0x7e, 0xfc, 0xe5, 0xd7,
    0xb3, 0x7b, 0xf6, 0xf1, 0xff, 0xe3, 0xdb, 0xab, 0x4b, 0x96, 0x31, 0x62, 0xc4, 0x95, 0x37, 0x6e, 0xdc, 0xa5, 0x57,
    0xae, 0x41, 0x82, 0x19, 0x32, 0x64, 0xc8, 0x8d, 0x07, 0x0e, 0x1c, 0x38, 0x70, 0xe0, 0xdd, 0xa7, 0x53, 0xa6, 0x51,
    0xa2, 0x59, 0xb2, 0x79, 0xf2, 0xf9, 0xef, 0xc3, 0x9b, 0x2b, 0x56, 0xac, 0x45, 0x8a, 0x09, 0x12, 0x24, 0x48, 0x90,
    0x3d, 0x7a, 0xf4, 0xf5, 0xf7, 0xf3, 0xfb, 0xeb, 0xcb, 0x8b, 0x0b, 0x16, 0x2c, 0x58, 0xb0, 0x7d, 0xfa, 0xe9, 0xcf,
    0x83, 0x1b, 0x36, 0x6c, 0xd8, 0xad, 0x47, 0x8e
])

function gfMul(a: number, b: number): number {
    if (a === 0 || b === 0) return 0
    return EXP_TABLE[(LOG_TABLE[a] + LOG_TABLE[b]) % 255]
}

function gfDiv(a: number, b: number): number {
    if (a === 0) return 0
    return EXP_TABLE[(LOG_TABLE[a] - LOG_TABLE[b] + 255) % 255]
}

function gfExp(a: number, n: number): number {
    if (n === 0) return 1
    if (a === 0) return 0
    return EXP_TABLE[(LOG_TABLE[a] * n) % 255]
}

// Matrix over GF(2^8), stored row-major in a flat Uint8Array
class GFMatrix {
    readonly rows: number
    readonly cols: number
    private data: Uint8Array

    constructor(rows: number, cols: number, data?: Uint8Array) {
        this.rows = rows
        this.cols = cols
        this.data = data ?? new Uint8Array(rows * cols)
    }

    get(r: number, c: number): number {
        return this.data[r * this.cols + c]
    }

    set(r: number, c: number, v: number): void {
        this.data[r * this.cols + c] = v
    }

    static identity(n: number): GFMatrix {
        const m = new GFMatrix(n, n)
        for (let i = 0; i < n; i++) m.set(i, i, 1)
        return m
    }

    // vandermonde[r][c] = gfExp(r, c)
    static vandermonde(rows: number, cols: number): GFMatrix {
        const m = new GFMatrix(rows, cols)
        for (let r = 0; r < rows; r++) {
            for (let c = 0; c < cols; c++) {
                m.set(r, c, gfExp(r, c))
            }
        }
        return m
    }

    multiply(right: GFMatrix): GFMatrix {
        const result = new GFMatrix(this.rows, right.cols)
        for (let r = 0; r < this.rows; r++) {
            for (let c = 0; c < right.cols; c++) {
                let val = 0
                for (let i = 0; i < this.cols; i++) {
                    val ^= gfMul(this.get(r, i), right.get(i, c))
                }
                result.set(r, c, val)
            }
        }
        return result
    }

    augment(right: GFMatrix): GFMatrix {
        const result = new GFMatrix(this.rows, this.cols + right.cols)
        for (let r = 0; r < this.rows; r++) {
            for (let c = 0; c < this.cols; c++) result.set(r, c, this.get(r, c))
            for (let c = 0; c < right.cols; c++) result.set(r, this.cols + c, right.get(r, c))
        }
        return result
    }

    subMatrix(rmin: number, cmin: number, rmax: number, cmax: number): GFMatrix {
        const result = new GFMatrix(rmax - rmin, cmax - cmin)
        for (let r = rmin; r < rmax; r++) {
            for (let c = cmin; c < cmax; c++) {
                result.set(r - rmin, c - cmin, this.get(r, c))
            }
        }
        return result
    }

    swapRows(r1: number, r2: number): void {
        for (let c = 0; c < this.cols; c++) {
            const tmp = this.get(r1, c)
            this.set(r1, c, this.get(r2, c))
            this.set(r2, c, tmp)
        }
    }

    invert(): GFMatrix {
        const size = this.rows
        const work = this.augment(GFMatrix.identity(size))
        work.gaussianElimination()
        return work.subMatrix(0, size, size, size * 2)
    }

    private gaussianElimination(): void {
        const rows = this.rows
        const cols = this.cols
        for (let r = 0; r < rows; r++) {
            if (this.get(r, r) === 0) {
                for (let rowBelow = r + 1; rowBelow < rows; rowBelow++) {
                    if (this.get(rowBelow, r) !== 0) {
                        this.swapRows(r, rowBelow)
                        break
                    }
                }
            }
            if (this.get(r, r) === 0) throw new Error('matrix is singular')
            if (this.get(r, r) !== 1) {
                const scale = gfDiv(1, this.get(r, r))
                for (let c = 0; c < cols; c++) {
                    this.set(r, c, gfMul(this.get(r, c), scale))
                }
            }
            for (let rowBelow = r + 1; rowBelow < rows; rowBelow++) {
                if (this.get(rowBelow, r) !== 0) {
                    const scale = this.get(rowBelow, r)
                    for (let c = 0; c < cols; c++) {
                        this.set(rowBelow, c, this.get(rowBelow, c) ^ gfMul(scale, this.get(r, c)))
                    }
                }
            }
        }
        for (let d = 0; d < rows; d++) {
            for (let rowAbove = 0; rowAbove < d; rowAbove++) {
                if (this.get(rowAbove, d) !== 0) {
                    const scale = this.get(rowAbove, d)
                    for (let c = 0; c < cols; c++) {
                        this.set(rowAbove, c, this.get(rowAbove, c) ^ gfMul(scale, this.get(d, c)))
                    }
                }
            }
        }
    }
}

// Builds the encoding matrix (Vandermonde × inverse of top square) and caches it.
// This is the default matrix from klauspost/reedsolomon's buildMatrix function. Row i is the
// combination of data shards that produces shard i, so the first dataShards rows are the
// identity and the rest are the parity rows. Encoding needs only the parity rows; reconstruction
// needs the rows of whichever shards survived, hence the whole matrix is what gets cached.
const encodingMatrixCache = new Map<string, GFMatrix>()

function getEncodingMatrix(dataShards: number, parityShards: number): GFMatrix {
    const key = `${dataShards},${parityShards}`
    let matrix = encodingMatrixCache.get(key)
    if (!matrix) {
        const vm = GFMatrix.vandermonde(dataShards + parityShards, dataShards)
        const top = vm.subMatrix(0, 0, dataShards, dataShards)
        matrix = vm.multiply(top.invert())
        encodingMatrixCache.set(key, matrix)
    }
    return matrix
}

const parityRowsCache = new Map<string, number[][]>()

function getParityRows(dataShards: number, parityShards: number): number[][] {
    const key = `${dataShards},${parityShards}`
    let rows = parityRowsCache.get(key)
    if (!rows) {
        const matrix = getEncodingMatrix(dataShards, parityShards)
        rows = []
        for (let i = 0; i < parityShards; i++) {
            const row: number[] = []
            for (let c = 0; c < dataShards; c++) {
                row.push(matrix.get(dataShards + i, c))
            }
            rows.push(row)
        }
        parityRowsCache.set(key, rows)
    }
    return rows
}

// Encodes data shards into parityCount parity shards.
// Each input shard and each output shard is a Uint8Array of the same length.
// Data shards are not modified. Compatible with klauspost/reedsolomon's default encoder.
export function rsEncode(data: Uint8Array[], parityCount: number): Uint8Array[] {
    if (parityCount === 0) return []
    const dataCount = data.length
    const shardSize = data[0].length
    const rows = getParityRows(dataCount, parityCount)
    return rows.map(row => {
        const parity = new Uint8Array(shardSize)
        for (let j = 0; j < shardSize; j++) {
            let val = 0
            for (let k = 0; k < dataCount; k++) {
                val ^= gfMul(row[k], data[k][j])
            }
            parity[j] = val
        }
        return parity
    })
}

// Rebuilds the missing data shards of one erasure batch from whatever survived.
//
// `shards` is indexed by shard position: 0..dataCount-1 are the data shards, the next
// parityCount are the parity shards, and null marks one that could not be retrieved. Returns a
// copy with the missing data shards filled in, or null when the batch is beyond repair — fewer
// than dataCount shards survived, or the survivors disagree on length.
//
// Ported from klauspost/reedsolomon's Reconstruct: shard_i = matrix_i · data, so taking the
// matrix rows of any dataCount survivors gives a square system whose inverse recovers the data
// vector — and the data vector is the data shards themselves.
export function rsReconstruct(
    shards: Array<Uint8Array | null>,
    dataCount: number,
    parityCount: number
): Array<Uint8Array | null> | null {
    const missing: number[] = []
    for (let i = 0; i < dataCount; i++) {
        if (!shards[i]) missing.push(i)
    }
    if (missing.length === 0) return shards.slice()

    const available: number[] = []
    for (let i = 0; i < dataCount + parityCount && available.length < dataCount; i++) {
        if (shards[i]) available.push(i)
    }
    if (available.length < dataCount) return null

    // Reed-Solomon works column by column across equally sized shards; a survivor of a different
    // length would silently shift every byte after it.
    const shardSize = shards[available[0]]!.length
    if (available.some(index => shards[index]!.length !== shardSize)) return null

    const matrix = getEncodingMatrix(dataCount, parityCount)
    const square = new GFMatrix(dataCount, dataCount)
    for (let r = 0; r < dataCount; r++) {
        for (let c = 0; c < dataCount; c++) {
            square.set(r, c, matrix.get(available[r], c))
        }
    }
    const decodeMatrix = square.invert()

    const result = shards.slice()
    for (const index of missing) {
        const shard = new Uint8Array(shardSize)
        for (let byte = 0; byte < shardSize; byte++) {
            let value = 0
            for (let k = 0; k < dataCount; k++) {
                value ^= gfMul(decodeMatrix.get(index, k), shards[available[k]]![byte])
            }
            shard[byte] = value
        }
        result[index] = shard
    }
    return result
}

// Parity lookup tables ported from Bee's pkg/file/redundancy/level.go.
// Each entry is [shardThresholds (descending), correspondingParityCounts].
// getParities(shards) finds the first threshold ≤ shards and returns its parity count.
const ERASURE_TABLES: [number[], number[]][] = [
    [[], []], // NONE (0)
    [
        [95, 69, 47, 29, 15, 6, 2, 1],
        [9, 8, 7, 6, 5, 4, 3, 2]
    ], // MEDIUM (1)
    [
        [105, 96, 87, 78, 70, 62, 54, 47, 40, 33, 27, 21, 16, 11, 7, 4, 2, 1],
        [21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4]
    ], // STRONG (2)
    [
        [93, 88, 83, 78, 74, 69, 64, 60, 55, 51, 46, 42, 38, 34, 30, 27, 23, 20, 17, 14, 11, 9, 6, 4, 3, 2, 1],
        [31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5]
    ], // INSANE (3)
    [
        [
            37, 36, 35, 34, 33, 32, 31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11,
            10, 9, 8, 7, 6, 5, 4, 3, 2, 1
        ],
        [
            89, 87, 86, 84, 83, 81, 80, 78, 76, 75, 73, 71, 70, 68, 66, 65, 63, 61, 59, 58, 56, 54, 52, 50, 48, 47, 45,
            43, 40, 38, 36, 34, 31, 29, 26, 23, 19
        ]
    ] // PARANOID (4)
]

const ENC_ERASURE_TABLES: [number[], number[]][] = [
    [[], []], // NONE (0)
    [
        [47, 34, 23, 14, 7, 3, 1],
        [9, 8, 7, 6, 5, 4, 3]
    ], // encMEDIUM (1)
    [
        [52, 48, 43, 39, 35, 31, 27, 23, 20, 16, 13, 10, 8, 5, 3, 2, 1],
        [21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5]
    ], // encSTRONG (2)
    [
        [46, 44, 41, 39, 37, 34, 32, 30, 27, 25, 23, 21, 19, 17, 15, 13, 11, 10, 8, 7, 5, 4, 3, 2, 1],
        [31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 6]
    ], // encINSANE (3)
    [
        [18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
        [87, 84, 81, 78, 75, 71, 68, 65, 61, 58, 54, 50, 47, 43, 38, 34, 29, 23]
    ] // encPARANOID (4)
]

function tableGetParities(table: [number[], number[]], shards: number): number {
    const [thresholds, parities] = table
    for (let i = 0; i < thresholds.length; i++) {
        if (shards >= thresholds[i]) return parities[i]
    }
    return 0
}

export function getParities(level: number, shards: number, encrypted: boolean): number {
    if (level <= 0 || level > 4) return 0
    return tableGetParities(encrypted ? ENC_ERASURE_TABLES[level] : ERASURE_TABLES[level], shards)
}

const BRANCHES = 128
const ENC_BRANCHES = 64

// Returns the max number of data shards per batch for a given level.
// Matches Bee's Level.GetMaxShards() and Level.GetMaxEncShards().
export function getMaxShards(level: number, encrypted: boolean): number {
    if (level <= 0) return encrypted ? ENC_BRANCHES : BRANCHES
    if (encrypted) {
        const p = getParities(level, ENC_BRANCHES, true)
        return Math.floor((BRANCHES - p) / 2)
    }
    return BRANCHES - getParities(level, BRANCHES, false)
}

// Constructs a Chunk from a 4104-byte raw shard (8-byte LE span + 4096-byte data).
function chunkFromBytes(bytes: Uint8Array): Chunk {
    let span = 0n
    for (let i = 7; i >= 0; i--) {
        span = span * 256n + BigInt(bytes[i])
    }
    const chunk = new Chunk(span)
    chunk.writer.buffer.set(bytes.subarray(8, 4104))
    return chunk
}

// Returns an onIntermediateChunk callback for ChunkSplitter that encodes the redundancy
// level into bit 7 of span byte 7 whenever the intermediate chunk contains parity refs.
// Matches Bee's redundancy.EncodeLevel: span[7] = level | 0x80
// This allows Bee's joiner to locate parity refs and perform RS reconstruction.
export function makeIntermediateChunkHandler(level: number): (chunk: Chunk, hasParity: boolean) => void {
    return (chunk: Chunk, hasParity: boolean) => {
        if (hasParity && level > 0) {
            chunk.span = (chunk.span & 0x00ffffffffffffffn) | (BigInt(level | 0x80) << 56n)
        }
    }
}

// Returns an onBatch callback that:
//  1. Uploads all data chunks in the batch via onChunk
//  2. Computes RS parity shards from the data chunk bytes
//  3. Uploads parity chunks via onChunk
//  4. Returns parity ChunkEntry[] for the splitter to include in the parent tree node
//
// Note: encrypted erasure coding is not yet supported; for encrypted=true the function
// uploads data chunks as normal and returns no parities (TODO: port encrypted RS path).
export function makeErasureBatch(
    level: number,
    encrypted: boolean,
    onChunk: (chunk: Chunk, key?: Uint8Array) => Promise<void>
): (batch: ChunkEntry[]) => Promise<ChunkEntry[]> {
    return async (batch: ChunkEntry[]): Promise<ChunkEntry[]> => {
        for (const { chunk, key } of batch) {
            await onChunk(chunk, key)
        }
        if (level <= 0) return []

        if (encrypted) {
            const parityCount = getParities(level, batch.length, true)
            if (parityCount === 0) return []
            const shardBytes = batch.map(({ chunk, key }) =>
                Binary.concatBytes(
                    Chunk.encryptSpan(key!, Binary.numberToUint64(chunk.span, 'LE')),
                    Chunk.encryptData(key!, chunk.writer.buffer)
                )
            )
            const parityShards = rsEncode(shardBytes, parityCount)
            const parityEntries: ChunkEntry[] = []
            for (const bytes of parityShards) {
                const parityChunk = chunkFromBytes(bytes)
                await onChunk(parityChunk)
                parityEntries.push({ chunk: parityChunk })
            }
            return parityEntries
        }

        const parityCount = getParities(level, batch.length, false)
        if (parityCount === 0) return []
        const dataBytes = batch.map(({ chunk }) => chunk.build())
        const parityShards = rsEncode(dataBytes, parityCount)
        const parityEntries: ChunkEntry[] = []
        for (const bytes of parityShards) {
            const chunk = chunkFromBytes(bytes)
            await onChunk(chunk)
            parityEntries.push({ chunk })
        }
        return parityEntries
    }
}
