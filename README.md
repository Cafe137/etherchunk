# Etherchunk

Etherchunk is an upload layer on top of Bee that stamps chunks client-side and tracks which slots in a postage batch are occupied by each file. This makes it possible to "delete" a file by reclaiming its slots for future uploads — something Swarm has no native concept of.

## Installation

```sh
npm install --global etherchunk
```

## Usage

See [Postage Batch Setup](docs/postage-batch-setup.md) for how to obtain `ETHERCHUNK_SIGNER`, `ETHERCHUNK_BATCH_ID`, and `ETHERCHUNK_BATCH_DEPTH`.

Environment variables can be set in your shell profile for global use, or in a local `.env` file if you prefer per-project configuration:

```sh
export ETHERCHUNK_UPLOAD_URL="http://localhost:1633/chunks"
export ETHERCHUNK_SIGNER="<private key hex>"
export ETHERCHUNK_BATCH_ID="<batch id hex>"
export ETHERCHUNK_BATCH_DEPTH=<depth of your batch>
export ETHERCHUNK_REDUNDANCY_LEVEL=0  # 0=none, 1=MEDIUM, 2=STRONG, 3=INSANE, 4=PARANOID
export ETHERCHUNK_PARALLELISM=32  # max chunks uploaded concurrently

# Optional — where `etherchunk export` reads chunks from. Defaults to ETHERCHUNK_UPLOAD_URL,
# since downloads are GET /chunks/<address> on the same endpoint uploads POST to.
export ETHERCHUNK_DOWNLOAD_URL="http://localhost:1633/chunks"
```

```sh
# Upload a file or directory and print the manifest root hash
etherchunk upload <file|dir>

# Upload with client-side encryption
etherchunk upload <file|dir> --encrypt

# Upload with erasure coding for redundancy (levels 1–4: MEDIUM, STRONG, INSANE, PARANOID)
etherchunk upload <file|dir> --redundancy=1

# Upload with both encryption and erasure coding
etherchunk upload <file|dir> --encrypt --redundancy=2

# Upload with a custom number of chunks in flight at once (default 32)
etherchunk upload <file|dir> --parallelism=64

# List all tracked files and manifests
etherchunk list

# Download everything ever uploaded from this batch, each upload into its own
# <dir>/<root hash>/ folder (default dir: ./etherchunk-export)
etherchunk export [dir]

# Export with a custom number of chunks in flight at once (default 32)
etherchunk export [dir] --parallelism=64

# Delete a file or manifest by root hash, reclaiming its slots
etherchunk delete <root hash>

# Show slot usage and most utilized bucket
etherchunk status

# Benchmark chunk splitting speed (no upload, no state changes)
etherchunk bench:split <file|dir>

# Benchmark chunk splitting + stamp signing speed (no upload, no state changes)
# Requires ETHERCHUNK_SIGNER, ETHERCHUNK_BATCH_ID, and ETHERCHUNK_BATCH_DEPTH
etherchunk bench:sign <file|dir>
```

State is stored in `~/.etherchunk/`, with files named by the first 8 hex characters of the batch ID (e.g. `etherchunk-a1b2c3d4.free`, `etherchunk-a1b2c3d4.db`). Multiple postage batches can be used independently by switching `ETHERCHUNK_BATCH_ID`.

## Design

Etherchunk stamps chunks client-side, which means it controls which `(bucket, slot)` each chunk is assigned to before sending the pre-signed chunk to the Bee API. This makes it possible to maintain a local index that tracks exactly which slots are occupied by each file, and to deliberately target previously freed slots when uploading new files.

All state lives under a `.etherchunk/` directory.

### `etherchunk.free` — SlotMap

A bitmap tracking slot occupancy across the entire batch. Each bit represents one slot: 0 = free, 1 = occupied. Pre-allocated at init time as all zeros, so the full batch capacity is available from the start.

```
per bucket (65536 entries, indexed by bucket number):
  [slot bitmap: slotsPerBucket / 8 bytes]
```

The fixed-size layout allows O(1) access by bucket index. Allocating a slot scans the bucket's bitmap for the first 0 bit and sets it to 1. Freeing a slot clears the corresponding bit. File size is determined by batch depth: for depth 24 (256 slots per bucket), the file is 2 MB.

### `etherchunk.db` — FileRegistry

A SQLite database tracking all uploaded files, their root chunk hash, and the slots they occupy.

```sql
CREATE TABLE files (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    path             TEXT NOT NULL,
    root_hash        BLOB NOT NULL,    -- 32 bytes
    chunks           BLOB NOT NULL,    -- repeated [bucket uint16, slot uint16] pairs
    kind             TEXT NOT NULL DEFAULT 'file',  -- 'file' or 'manifest'
    redundancy_level INTEGER NOT NULL DEFAULT 0,    -- 0=none, 1=MEDIUM, 2=STRONG, 3=INSANE, 4=PARANOID
    upload_date      INTEGER                        -- unix timestamp, nullable
);
CREATE INDEX idx_root_hash ON files(root_hash);
```

Chunk refs are packed as 4 bytes each (`[bucket uint16][slot uint16]`), so overhead is approximately 4 bytes per chunk — about 0.1% of the file size — regardless of file size. The index on `root_hash` makes lookup and deletion O(log N) in the number of tracked files.

### Manifest upload flow

`upload <dir>` packages a directory as a Swarm website so it can be browsed via the Bzz gateway (`/bzz/<root-hash>/`).

It builds a [Mantaray v0.2](https://github.com/ethersphere/bee/tree/master/pkg/manifest/mantaray) trie — the same binary format Bee uses natively:

1. **Pass 1 — upload content**: walk the directory, split each file into chunks, stamp and upload each chunk, collect the root hash per file
2. **Pass 2 — build the trie**: construct a Mantaray node for every file path (no leading slash, e.g. `index.html`), plus a `'/'` metadata node carrying `website-index-document` if `index.html` is present — this is what Bee reads to serve the index page
3. **Upload the trie**: serialize each trie node into a chunk, stamp and upload it, record the manifest root hash in `etherchunk.db`

After upload, the directory is accessible at `<gateway>/bzz/<root-hash>/` and `<gateway>/bzz/<root-hash>/path/to/file`.

### Upload flow

1. Split the file into chunks client-side
2. For each chunk, allocate a slot from `etherchunk.free` for the matching bucket
3. Sign the stamp with the chosen `(bucket, slot)` and send the pre-signed chunk to Bee
4. Wrap the file in a single-entry Mantaray manifest (with a `website-index-document` pointer) so the file is directly browseable at `<gateway>/bzz/<root-hash>/`
5. Record the manifest root hash and all `(bucket, slot)` pairs in `etherchunk.db`

### Deletion flow

1. Look up the file's chunk list in `etherchunk.db`
2. Return all its `(bucket, slot)` pairs to `etherchunk.free` under their respective buckets
3. Remove the file entry from `etherchunk.db`
4. Optionally upload tombstone chunks to overwrite the slots on the network

### Export flow

`export` reverses an upload for every entry in `etherchunk.db`, reading chunks back with `GET /chunks/<address>` and reassembling them locally — nothing about the reconstruction is delegated to the node.

1. For each registry entry, create `<dir>/<root hash>/`
2. Walk the Mantaray trie from the root reference to recover every file path and its reference
3. Join each file's chunk tree and stream the result into `<dir>/<root hash>/<path>`

Every chunk is verified against the address it was fetched by, so a wrong endpoint or a corrupted body can never end up inside an exported file. Reassembly follows chunk spans rather than counting references, which is what lets it stop before the parity references that an erasure-coded upload appends to its intermediate chunks. Encrypted uploads need no extra input: the 64-byte reference recorded at upload time carries the key alongside the address.

An entry whose chunks are no longer retrievable is reported on its own line and does not stop the remaining entries — postage batches routinely outlive the content of individual uploads.

### Recovering damaged uploads

If an upload was made with `--redundancy`, `export` uses it. Chunks that are missing — or that come back corrupted, which amounts to the same thing once the address check fails — are rebuilt from what the upload paid for:

-   **Reed-Solomon**, for anything inside a chunk tree. An erasure batch is the set of children of one intermediate chunk, so a batch that lost no more chunks than it has parity shards is rebuilt in place. At `--redundancy=4` there are more parity shards than data shards, so an entire batch can be gone and still come back.
-   **Dispersed replicas**, for root chunks. Parity references live in the parent of the chunks they protect, so the top of a tree has none; each file root and each Mantaray node's root are covered by their SOC replicas instead.

The Mantaray trie is protected exactly like the content it points at — parity inside any node wide enough to span several chunks, replicas for every node's root chunk. Losing one trie chunk would otherwise lose every file below it, whatever the content's parity.

Rebuilt chunks are checked against the address they were meant to have before anything is written, so recovery can fail loudly but cannot produce a wrong file. When it happens, export says so:

```
9f86d0…  1 file(s), 499712 bytes  ->  ./etherchunk-export/9f86d0…
          recovered 10 from parity, 1 from dispersed replicas
```

A successful export with a non-zero recovery count means the content is intact but the upload is decaying on the network — worth re-uploading before the parity runs out.
