const ADDRESS_LENGTH = 32

// Small enough that a one-chunk upload doesn't allocate a big table; it doubles from here.
// Must be a power of two so an index is a mask away, not a modulo.
const INITIAL_CAPACITY = 1024

// A chunk address is already a BMT/keccak hash, i.e. uniformly distributed bits, so the table
// index is taken straight from it — hashing a hash would cost work and buy no spread. The
// *low* end is used because the top 16 bits are the chunk's bucket: indexing on those would
// pile one bucket's chunks onto consecutive indices, and those are exactly the addresses
// competing for the same slots. Masking makes the sign of the 24-bit shift irrelevant.
function tableIndex(bytes: Uint8Array, offset: number, mask: number): number {
    return (bytes[offset + 31] | (bytes[offset + 30] << 8) | (bytes[offset + 29] << 16) | (bytes[offset + 28] << 24)) & mask
}

function addressEquals(a: Uint8Array, aOffset: number, b: Uint8Array, bOffset: number): boolean {
    for (let i = 0; i < ADDRESS_LENGTH; i++) {
        if (a[aOffset + i] !== b[bOffset + i]) {
            return false
        }
    }
    return true
}

/**
 * Remembers which slot a chunk address was already given, so an address repeated within one
 * upload consumes one slot and one POST instead of one per occurrence. Identical content
 * hashes to an identical address and Bee stores chunks by address, so N references to one
 * stored copy is what the tree already means — allocating a fresh slot per occurrence just
 * burned batch capacity, and a file repeating one block more times than a bucket has slots
 * could not be uploaded at all.
 *
 * Deliberately *not* persisted and never shared between uploads: every reference to a
 * deduplicated slot then lives in a single registry row, so `delete` still frees exactly the
 * slots nothing else points at. A cross-upload memo would break that.
 *
 * Exact, never lossy — a repeated address always finds its slot, so "more repeats of one block
 * than a bucket has slots" cannot fail again. The cost is ~70 bytes of live table per
 * *distinct* chunk (35 per index, held at half load), measured at ~155 bytes of peak RSS
 * because doubling briefly holds both tables and the freed pages are not returned. A capped,
 * evicting cache would bound that, but only by making the guarantee probabilistic, and it
 * would not rescue an upload whose per-occurrence ref array is already too big to fit. If a
 * very large upload ever needs the memory back, storing a 16-byte prefix instead of the whole
 * address halves it and stays exact for every practical purpose.
 *
 * Open addressing with linear probing. Entries are only ever added or overwritten in place,
 * never removed, so there are no tombstones and a run of occupied indices is unbroken: a
 * lookup can stop at the first empty index it sees.
 */
export class ChunkMemo {
    private capacity = INITIAL_CAPACITY
    private addresses = new Uint8Array(INITIAL_CAPACITY * ADDRESS_LENGTH)
    private slots = new Uint16Array(INITIAL_CAPACITY)
    private occupied = new Uint8Array(INITIAL_CAPACITY)
    private entries = 0

    get size(): number {
        return this.entries
    }

    get(address: Uint8Array): number | undefined {
        const mask = this.capacity - 1
        let index = tableIndex(address, 0, mask)
        while (this.occupied[index]) {
            if (addressEquals(this.addresses, index * ADDRESS_LENGTH, address, 0)) {
                return this.slots[index]
            }
            index = (index + 1) & mask
        }
        return undefined
    }

    set(address: Uint8Array, slot: number): void {
        // Half load is what keeps probe runs short; it also guarantees the loops below find an
        // empty index and terminate.
        if (this.entries * 2 >= this.capacity) {
            this.grow()
        }
        if (insert(this.addresses, this.slots, this.occupied, this.capacity, address, 0, slot)) {
            this.entries++
        }
    }

    private grow(): void {
        const capacity = this.capacity * 2
        const addresses = new Uint8Array(capacity * ADDRESS_LENGTH)
        const slots = new Uint16Array(capacity)
        const occupied = new Uint8Array(capacity)
        for (let index = 0; index < this.capacity; index++) {
            if (this.occupied[index]) {
                insert(addresses, slots, occupied, capacity, this.addresses, index * ADDRESS_LENGTH, this.slots[index])
            }
        }
        this.capacity = capacity
        this.addresses = addresses
        this.slots = slots
        this.occupied = occupied
    }
}

// Places one address into `addresses`/`slots`, reading it from `source` at `sourceOffset` so
// both a caller-supplied address and an already-stored one (during growth) can be inserted.
// Returns whether it claimed a previously free index, i.e. whether the table gained an entry.
function insert(
    addresses: Uint8Array,
    slots: Uint16Array,
    occupied: Uint8Array,
    capacity: number,
    source: Uint8Array,
    sourceOffset: number,
    slot: number
): boolean {
    const mask = capacity - 1
    let index = tableIndex(source, sourceOffset, mask)
    while (occupied[index]) {
        if (addressEquals(addresses, index * ADDRESS_LENGTH, source, sourceOffset)) {
            slots[index] = slot
            return false
        }
        index = (index + 1) & mask
    }
    occupied[index] = 1
    addresses.set(source.subarray(sourceOffset, sourceOffset + ADDRESS_LENGTH), index * ADDRESS_LENGTH)
    slots[index] = slot
    return true
}
