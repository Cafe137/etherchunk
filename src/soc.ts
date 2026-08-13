import { Binary, Chunk, Elliptic } from 'cafe-utility'

// Private key [1, 0, ..., 0] — the fixed signer used for all dispersed replicas.
// Its Ethereum address is REPLICAS_OWNER below.
const REPLICAS_PRIVATE_KEY: bigint = (() => {
    const k = new Uint8Array(32)
    k[0] = 1
    return Binary.uint256ToNumber(k, 'BE')
})()

// Ethereum address derived from REPLICAS_PRIVATE_KEY — constant across all Bee nodes.
export const REPLICAS_OWNER = Binary.hexToUint8Array('dc5b20847f43d67928f49cd4f85d696b5a7617b5')

// Replica counts per redundancy level: NONE=0, MEDIUM=2, STRONG=4, INSANE=8, PARANOID=16
const REPLICA_COUNTS = [0, 2, 4, 8, 16]

// "\x19Ethereum Signed Message:\n32" (same prefix as stamper.ts)
const SIGNATURE_PREFIX = new Uint8Array([
    25, 69, 116, 104, 101, 114, 101, 117, 109, 32, 83, 105, 103, 110, 101, 100, 32, 77, 101, 115, 115, 97, 103, 101, 58,
    10, 51, 50
])

// Base offsets for neighbourhood index computation, one per erasure level 1..4.
// Ported from Bee's replicas.go: var replicaIndexBases = [5]int{0, 2, 6, 14}
const NH_BASES = [0, 2, 6, 14]

/**
 * SOC address = keccak256(id || owner).
 * Ported from Bee's soc.go CreateAddress / hash(id, owner).
 */
export function socAddress(id: Uint8Array, owner: Uint8Array): Uint8Array {
    return Binary.keccak256(Binary.concatBytes(id, owner))
}

/**
 * Returns the neighbourhood index used to disperse replicas.
 * Ported from Bee's replicas.go nh():
 *   replicaIndexBases[d-1] + int(addr[0] >> (8 - d))
 * where d = redundancyLevel (1..4).
 */
function nhIndex(redundancyLevel: number, addr: Uint8Array): number {
    return NH_BASES[redundancyLevel - 1] + (addr[0] >> (8 - redundancyLevel))
}

/**
 * Computes the SOC IDs for all dispersed replicas of a root chunk at the given
 * redundancy level.
 *
 * Each ID is the root address with byte 0 replaced by an incrementing counter.
 * We collect the first ID that lands in each distinct d-bit neighbourhood of the
 * resulting SOC address, until we have REPLICA_COUNTS[level] IDs.
 *
 * Ported from Bee's replicas.go replicator.replicas() / replicate() / add().
 */
function replicaIds(rootAddress: Uint8Array, redundancyLevel: number): Uint8Array[] {
    const count = REPLICA_COUNTS[redundancyLevel]
    if (count === 0) return []

    const covered = new Set<number>()
    const ids: Uint8Array[] = []

    for (let i = 0; i < 255 && ids.length < count; i++) {
        const id = new Uint8Array(32)
        id.set(rootAddress)
        id[0] = i

        const addr = socAddress(id, REPLICAS_OWNER)
        const nh = nhIndex(redundancyLevel, addr)

        if (!covered.has(nh)) {
            covered.add(nh)
            ids.push(id)
        }
    }

    return ids
}

// A SOC is id (32) || signature (65) || span (8) || payload, so the wrapped chunk body — span
// included — starts here. Reading a replica means slicing this off and checking that what is left
// hashes to the address that was wanted; that check subsumes verifying the signature, since a
// forged replica cannot produce content-addressed bytes it does not have.
export const SOC_HEADER_SIZE = 97

/**
 * The SOC addresses of every dispersed replica of a chunk, in the same order makeReplicas
 * created them. Read side of makeReplicas / makeEncryptedReplicas: pass the address the replicas
 * were built from — the plain BMT hash, or the encrypted address for an encrypted upload.
 */
export function replicaAddresses(chunkAddress: Uint8Array, redundancyLevel: number): Uint8Array[] {
    return replicaIds(chunkAddress, redundancyLevel).map(id => socAddress(id, REPLICAS_OWNER))
}

/**
 * Builds the raw bytes for a signed SOC chunk that wraps `wrappedChunk`.
 *
 * Layout (ported from Bee's soc.go toBytes()):
 *   id (32) || signature (65) || span (8) || payload (≤4096)
 *
 * Signing (ported from Bee's soc.go Sign() + crypto/signer.go):
 *   toSign  = keccak256(id || cacAddress)
 *   digest  = SIGNATURE_PREFIX || toSign    (same pattern as stamper.ts)
 *   sign digest with Elliptic.signMessage → (r, s, v)
 */
export function makeSocChunk(
    id: Uint8Array,
    wrappedChunk: Chunk,
    signerKey: bigint = REPLICAS_PRIVATE_KEY
): { address: Uint8Array; data: Uint8Array } {
    const cacAddress = wrappedChunk.hash()
    const toSign = Binary.keccak256(Binary.concatBytes(id, cacAddress))
    const digest = Binary.concatBytes(SIGNATURE_PREFIX, toSign)
    const [r, s, v] = Elliptic.signMessage(digest, signerKey)
    const signature = Binary.concatBytes(
        Binary.numberToUint256(r, 'BE'),
        Binary.numberToUint256(s, 'BE'),
        new Uint8Array([Number(v)])
    )
    const data = Binary.concatBytes(id, signature, wrappedChunk.build())

    const pubKey = Elliptic.privateKeyToPublicKey(signerKey)
    const owner = Elliptic.publicKeyToAddress(pubKey)
    const address = socAddress(id, owner)

    return { address, data }
}

/**
 * Creates all dispersed replica SOC chunks for the given root chunk.
 * Returns an array of { address, data } ready for stamping and uploading.
 *
 * Returns empty array when redundancyLevel === 0 (NONE).
 */
export function makeReplicas(
    rootChunk: Chunk,
    redundancyLevel: number
): Array<{ address: Uint8Array; data: Uint8Array }> {
    if (redundancyLevel === 0) return []

    const rootAddress = rootChunk.hash()
    const ids = replicaIds(rootAddress, redundancyLevel)

    return ids.map(id => makeSocChunk(id, rootChunk, REPLICAS_PRIVATE_KEY))
}

/**
 * Builds a SOC chunk from pre-built raw bytes and an explicit CAC address.
 *
 * Used for encrypted replicas where the body is the encrypted chunk bytes
 * (not the plain chunk.build() output) and the CAC address is the encrypted
 * chunk address rather than the BMT hash of the unencrypted content.
 */
function makeSocChunkRaw(
    id: Uint8Array,
    cacAddress: Uint8Array,
    body: Uint8Array,
    signerKey: bigint = REPLICAS_PRIVATE_KEY
): { address: Uint8Array; data: Uint8Array } {
    const toSign = Binary.keccak256(Binary.concatBytes(id, cacAddress))
    const digest = Binary.concatBytes(SIGNATURE_PREFIX, toSign)
    const [r, s, v] = Elliptic.signMessage(digest, signerKey)
    const signature = Binary.concatBytes(
        Binary.numberToUint256(r, 'BE'),
        Binary.numberToUint256(s, 'BE'),
        new Uint8Array([Number(v)])
    )
    const data = Binary.concatBytes(id, signature, body)

    const pubKey = Elliptic.privateKeyToPublicKey(signerKey)
    const owner = Elliptic.publicKeyToAddress(pubKey)
    const address = socAddress(id, owner)

    return { address, data }
}

/**
 * Creates dispersed replica SOC chunks for an *encrypted* root chunk.
 *
 * For encrypted uploads the replica must wrap the encrypted chunk bytes
 * (encrypted span + encrypted payload) and IDs are derived from the
 * encrypted chunk address — matching Bee's behaviour.
 */
export function makeEncryptedReplicas(
    rootChunk: Chunk,
    key: Uint8Array,
    redundancyLevel: number
): Array<{ address: Uint8Array; data: Uint8Array }> {
    if (redundancyLevel === 0) return []

    const encryptedAddress = rootChunk.encryptedHash(key).address
    const ids = replicaIds(encryptedAddress, redundancyLevel)

    const encryptedBody = Binary.concatBytes(
        Chunk.encryptSpan(key, Binary.numberToUint64(rootChunk.span, 'LE')),
        Chunk.encryptData(key, rootChunk.writer.buffer)
    )

    return ids.map(id => makeSocChunkRaw(id, encryptedAddress, encryptedBody, REPLICAS_PRIVATE_KEY))
}
