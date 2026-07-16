import { existsSync, readFileSync, writeFileSync } from 'node:fs'

const BUCKET_COUNT = 65536

// The bucket depth is fixed at 16 (2^16 buckets), and a batch depth must exceed it, so 17
// (2 slots/bucket) is the smallest valid batch depth. The upper bound is where the flat
// bitmap stops being addressable with unsigned 32-bit bit indices (`bitIndex >>> 3`): depth
// 32 tops out at bit 2^32-1 exactly, a 512 MB file; depth 33 would overflow the shift AND
// need a >512 MB file no real batch approaches. Enforcing the range turns an out-of-range
// depth into a clear error instead of a cryptic RangeError or (past 32) silent index
// corruption — the very failure mode this module exists to prevent.
const MIN_DEPTH = 17
const MAX_DEPTH = 32

function assertSupportedDepth(depth: number): void {
    if (!Number.isInteger(depth) || depth < MIN_DEPTH || depth > MAX_DEPTH) {
        throw new Error(
            `Unsupported batch depth ${depth}: expected an integer between ${MIN_DEPTH} and ${MAX_DEPTH} ` +
                `(a depth-N batch has 2^(N-16) slots across ${BUCKET_COUNT} buckets).`
        )
    }
}

function slotsPerBucketForDepth(depth: number): number {
    return 1 << (depth - 16)
}

// The .free file is a flat bitmap of BUCKET_COUNT * slotsPerBucket bits, one bit per slot,
// addressed globally as `bucket * slotsPerBucket + slot`. Total slots is always a multiple
// of 8 (BUCKET_COUNT is), so the byte length is a whole number even when a single bucket
// spans fewer than 8 bits (depths 16-18). For byte-aligned depths (>= 19) this lays out
// bytes identically to a per-bucket-byte scheme, so existing files stay readable.
function byteLengthForDepth(depth: number): number {
    return (BUCKET_COUNT * slotsPerBucketForDepth(depth)) / 8
}

// A .free file's size is fully determined by BUCKET_COUNT and the batch depth it was
// created with, so the depth a file was created with can be read back out of its size.
// Used to detect a stale .free file after ETHERCHUNK_BATCH_DEPTH changes (e.g. after diluting
// a postage batch) and to drive migrate().
function inferDepthFromFileSize(path: string, fileSize: number): number {
    const totalSlots = fileSize * 8
    if (totalSlots % BUCKET_COUNT !== 0) {
        throw new Error(`Slot map file ${path} has an invalid size (${fileSize} bytes); it does not hold a whole number of slots per bucket (${BUCKET_COUNT} buckets)`)
    }
    const slotsPerBucket = totalSlots / BUCKET_COUNT
    const depth = 16 + Math.log2(slotsPerBucket)
    if (!Number.isInteger(depth) || depth < 16) {
        throw new Error(`Slot map file ${path} has an invalid size (${fileSize} bytes); cannot infer a valid depth from it`)
    }
    return depth
}

export interface MigrationResult {
    migrated: boolean
    reason?: string
    oldDepth?: number
    newDepth?: number
    oldSize?: number
    newSize?: number
    backupPath?: string
}

export class SlotMap {
    private data: Buffer
    private slotsPerBucket: number

    constructor(private path: string, depth: number) {
        assertSupportedDepth(depth)
        this.slotsPerBucket = slotsPerBucketForDepth(depth)
        const expectedSize = byteLengthForDepth(depth)
        if (existsSync(path)) {
            this.data = readFileSync(path)
            if (this.data.length !== expectedSize) {
                const foundDepth = inferDepthFromFileSize(path, this.data.length)
                throw new Error(
                    `Slot map file ${path} was created with depth ${foundDepth}, but ETHERCHUNK_BATCH_DEPTH is now ${depth}. ` +
                        `Run "etherchunk migrate" to safely convert the existing state before continuing.`
                )
            }
        } else {
            this.data = Buffer.alloc(expectedSize)
            writeFileSync(path, this.data)
        }
    }

    allocSlot(bucket: number): number {
        const baseBit = bucket * this.slotsPerBucket
        for (let slot = 0; slot < this.slotsPerBucket; slot++) {
            const bitIndex = baseBit + slot
            const byteIndex = bitIndex >>> 3
            const mask = 1 << (bitIndex & 7)
            if ((this.data[byteIndex] & mask) === 0) {
                this.data[byteIndex] |= mask
                return slot
            }
        }
        throw new Error(`Bucket 0x${bucket.toString(16).padStart(4, '0')} is full`)
    }

    freeSlot(bucket: number, slot: number): void {
        const bitIndex = bucket * this.slotsPerBucket + slot
        this.data[bitIndex >>> 3] &= ~(1 << (bitIndex & 7))
    }

    getStats() {
        const slotsPerBucket = this.slotsPerBucket
        const totalSlots = BUCKET_COUNT * slotsPerBucket
        let occupiedSlots = 0
        let mostUtilizedBucket = 0
        let mostUtilizedCount = 0
        if (slotsPerBucket >= 8) {
            // Byte-aligned depths (>= 19): each bucket owns a whole byte range, so count set
            // bits a byte at a time and skip empty bytes — this leaves `status` fast even on
            // deep, sparsely-filled batches (a per-slot scan would be ~8x the work).
            const bytesPerBucket = slotsPerBucket >> 3
            for (let bucket = 0; bucket < BUCKET_COUNT; bucket++) {
                const base = bucket * bytesPerBucket
                let bucketOccupied = 0
                for (let i = 0; i < bytesPerBucket; i++) {
                    let byte = this.data[base + i]
                    while (byte) {
                        bucketOccupied += byte & 1
                        byte >>= 1
                    }
                }
                occupiedSlots += bucketOccupied
                if (bucketOccupied > mostUtilizedCount) {
                    mostUtilizedCount = bucketOccupied
                    mostUtilizedBucket = bucket
                }
            }
        } else {
            // Sub-byte depths (17-18): several buckets share a byte, so count per slot bit.
            for (let bucket = 0; bucket < BUCKET_COUNT; bucket++) {
                const baseBit = bucket * slotsPerBucket
                let bucketOccupied = 0
                for (let slot = 0; slot < slotsPerBucket; slot++) {
                    const bitIndex = baseBit + slot
                    if (this.data[bitIndex >>> 3] & (1 << (bitIndex & 7))) {
                        bucketOccupied++
                    }
                }
                occupiedSlots += bucketOccupied
                if (bucketOccupied > mostUtilizedCount) {
                    mostUtilizedCount = bucketOccupied
                    mostUtilizedBucket = bucket
                }
            }
        }
        return {
            totalSlots,
            occupiedSlots,
            freeSlots: totalSlots - occupiedSlots,
            slotsPerBucket,
            mostUtilizedBucket,
            mostUtilizedCount
        }
    }

    save(): void {
        writeFileSync(this.path, this.data)
    }

    // Rewrites an existing .free file to match `newDepth`, preserving every occupied slot.
    // Bucket assignment (top 16 bits of a chunk's address) never changes with depth, and
    // slot numbers within a bucket are dense from 0 upward, so widening a bucket's bitmap
    // is just: copy its old bits into the low end of a bigger, zero-filled bitmap. Growing
    // depth (dilution) is always safe this way. Shrinking depth would truncate bits for
    // slots that may already be occupied, silently losing which chunks are allocated — so
    // it is refused rather than guessed at.
    static migrate(path: string, newDepth: number): MigrationResult {
        assertSupportedDepth(newDepth)
        if (!existsSync(path)) {
            return { migrated: false, reason: 'no existing slot map file to migrate' }
        }
        const oldData = readFileSync(path)
        const oldDepth = inferDepthFromFileSize(path, oldData.length)
        if (oldDepth === newDepth) {
            return { migrated: false, reason: 'already at target depth', oldDepth, newDepth }
        }
        if (newDepth < oldDepth) {
            throw new Error(
                `Refusing to migrate ${path} from depth ${oldDepth} down to ${newDepth}: shrinking would truncate ` +
                    `slot data and could lose track of already-occupied slots. Only growing the depth (batch dilution) is supported.`
            )
        }

        const backupPath = `${path}.bak-depth${oldDepth}`
        if (existsSync(backupPath)) {
            throw new Error(
                `Refusing to migrate: backup path ${backupPath} already exists. Move or remove it first if it's safe to discard.`
            )
        }

        const oldSlotsPerBucket = slotsPerBucketForDepth(oldDepth)
        const newSlotsPerBucket = slotsPerBucketForDepth(newDepth)
        const newData = Buffer.alloc(byteLengthForDepth(newDepth))
        if (oldSlotsPerBucket % 8 === 0 && newSlotsPerBucket % 8 === 0) {
            // Both depths give each bucket a whole number of bytes, so a bucket's bitmap can
            // be copied as an aligned byte range into the low end of its bigger new region.
            const oldBytesPerBucket = oldSlotsPerBucket / 8
            const newBytesPerBucket = newSlotsPerBucket / 8
            for (let bucket = 0; bucket < BUCKET_COUNT; bucket++) {
                oldData.copy(newData, bucket * newBytesPerBucket, bucket * oldBytesPerBucket, (bucket + 1) * oldBytesPerBucket)
            }
        } else {
            // A sub-byte depth (17-18) packs multiple buckets into a byte, so a byte-range
            // copy would smear neighbouring buckets together. Move each occupied slot bit
            // individually from its old global bit index to its new one.
            for (let bucket = 0; bucket < BUCKET_COUNT; bucket++) {
                const oldBase = bucket * oldSlotsPerBucket
                const newBase = bucket * newSlotsPerBucket
                for (let slot = 0; slot < oldSlotsPerBucket; slot++) {
                    const oldBit = oldBase + slot
                    if (oldData[oldBit >>> 3] & (1 << (oldBit & 7))) {
                        const newBit = newBase + slot
                        newData[newBit >>> 3] |= 1 << (newBit & 7)
                    }
                }
            }
        }

        writeFileSync(backupPath, oldData)
        writeFileSync(path, newData)

        return {
            migrated: true,
            oldDepth,
            newDepth,
            oldSize: oldData.length,
            newSize: newData.length,
            backupPath
        }
    }
}
