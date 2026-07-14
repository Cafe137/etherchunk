import { existsSync, readFileSync, writeFileSync } from 'node:fs'

const BUCKET_COUNT = 65536

function bytesPerBucketForDepth(depth: number): number {
    const slotsPerBucket = 1 << (depth - 16)
    return slotsPerBucket / 8
}

// A .free file's size is fully determined by BUCKET_COUNT and the batch depth it was
// created with, so the depth a file was created with can be read back out of its size.
// Used to detect a stale .free file after SWARMFS_BATCH_DEPTH changes (e.g. after diluting
// a postage batch) and to drive migrate().
function inferDepthFromFileSize(path: string, fileSize: number): number {
    if (fileSize % BUCKET_COUNT !== 0) {
        throw new Error(`Slot map file ${path} has an invalid size (${fileSize} bytes); it is not a multiple of the bucket count (${BUCKET_COUNT})`)
    }
    const bytesPerBucket = fileSize / BUCKET_COUNT
    const slotsPerBucket = bytesPerBucket * 8
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
    private bytesPerBucket: number

    constructor(private path: string, depth: number) {
        this.bytesPerBucket = bytesPerBucketForDepth(depth)
        if (existsSync(path)) {
            this.data = readFileSync(path)
            const expectedSize = BUCKET_COUNT * this.bytesPerBucket
            if (this.data.length !== expectedSize) {
                const foundDepth = inferDepthFromFileSize(path, this.data.length)
                throw new Error(
                    `Slot map file ${path} was created with depth ${foundDepth}, but SWARMFS_BATCH_DEPTH is now ${depth}. ` +
                        `Run "swarmfs migrate" to safely convert the existing state before continuing.`
                )
            }
        } else {
            this.data = Buffer.alloc(BUCKET_COUNT * this.bytesPerBucket)
            writeFileSync(path, this.data)
        }
    }

    allocSlot(bucket: number): number {
        const base = bucket * this.bytesPerBucket
        for (let i = 0; i < this.bytesPerBucket; i++) {
            const byte = this.data[base + i]
            if (byte === 0xff) continue
            for (let bit = 0; bit < 8; bit++) {
                if ((byte & (1 << bit)) === 0) {
                    this.data[base + i] |= 1 << bit
                    return i * 8 + bit
                }
            }
        }
        throw new Error(`Bucket 0x${bucket.toString(16).padStart(4, '0')} is full`)
    }

    freeSlot(bucket: number, slot: number): void {
        const base = bucket * this.bytesPerBucket
        this.data[base + Math.floor(slot / 8)] &= ~(1 << slot % 8)
    }

    getStats() {
        const slotsPerBucket = this.bytesPerBucket * 8
        const totalSlots = BUCKET_COUNT * slotsPerBucket
        let occupiedSlots = 0
        let mostUtilizedBucket = 0
        let mostUtilizedCount = 0
        for (let bucket = 0; bucket < BUCKET_COUNT; bucket++) {
            const base = bucket * this.bytesPerBucket
            let bucketOccupied = 0
            for (let i = 0; i < this.bytesPerBucket; i++) {
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
    // is just: copy its old bytes into the low end of a bigger, zero-filled bitmap. Growing
    // depth (dilution) is always safe this way. Shrinking depth would truncate bits for
    // slots that may already be occupied, silently losing which chunks are allocated — so
    // it is refused rather than guessed at.
    static migrate(path: string, newDepth: number): MigrationResult {
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

        const oldBytesPerBucket = bytesPerBucketForDepth(oldDepth)
        const newBytesPerBucket = bytesPerBucketForDepth(newDepth)
        const newData = Buffer.alloc(BUCKET_COUNT * newBytesPerBucket)
        for (let bucket = 0; bucket < BUCKET_COUNT; bucket++) {
            oldData.copy(newData, bucket * newBytesPerBucket, bucket * oldBytesPerBucket, (bucket + 1) * oldBytesPerBucket)
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
