import Database from 'better-sqlite3'

export interface ChunkRef {
    bucket: number
    slot: number
}

export type EntryKind = 'file' | 'manifest'

export class FileRegistry {
    private db: Database.Database

    constructor(path: string) {
        this.db = new Database(path)
        this.db.exec(`
            CREATE TABLE IF NOT EXISTS files (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                path             TEXT NOT NULL,
                root_hash        BLOB NOT NULL,
                chunks           BLOB NOT NULL,
                kind             TEXT NOT NULL DEFAULT 'file',
                redundancy_level INTEGER NOT NULL DEFAULT 0,
                upload_date      INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_root_hash ON files(root_hash);
        `)
        const cols = this.db.prepare('PRAGMA table_info(files)').all() as Array<{ name: string }>
        if (!cols.some(c => c.name === 'kind')) {
            this.db.exec("ALTER TABLE files ADD COLUMN kind TEXT NOT NULL DEFAULT 'file'")
        }
        if (!cols.some(c => c.name === 'redundancy_level')) {
            this.db.exec('ALTER TABLE files ADD COLUMN redundancy_level INTEGER NOT NULL DEFAULT 0')
        }
        if (!cols.some(c => c.name === 'upload_date')) {
            this.db.exec('ALTER TABLE files ADD COLUMN upload_date INTEGER')
        }
    }

    list(): Array<{
        path: string
        rootHash: Uint8Array
        kind: EntryKind
        chunkCount: number
        redundancyLevel: number
        uploadDate: number | null
    }> {
        const rows = this.db
            .prepare(
                'SELECT path, root_hash, kind, length(chunks) AS chunks_len, redundancy_level, upload_date FROM files'
            )
            .all() as Array<{
            path: string
            root_hash: Buffer
            kind: EntryKind
            chunks_len: number
            redundancy_level: number
            upload_date: number | null
        }>
        return rows.map(row => ({
            path: row.path,
            rootHash: row.root_hash,
            kind: row.kind,
            chunkCount: row.chunks_len / 4,
            redundancyLevel: row.redundancy_level,
            uploadDate: row.upload_date
        }))
    }

    add(
        path: string,
        rootHash: Uint8Array,
        chunks: ChunkRef[],
        kind: EntryKind = 'file',
        redundancyLevel = 0,
        uploadDate: number = Date.now()
    ): void {
        this.db
            .prepare(
                'INSERT INTO files (path, root_hash, chunks, kind, redundancy_level, upload_date) VALUES (?, ?, ?, ?, ?, ?)'
            )
            .run(path, rootHash, serializeChunks(chunks), kind, redundancyLevel, uploadDate)
    }

    removeByRootHash(rootHash: Uint8Array): ChunkRef[] | null {
        const row = this.db.prepare('SELECT id, chunks FROM files WHERE root_hash = ? LIMIT 1').get(rootHash) as
            | { id: number; chunks: Buffer }
            | undefined
        if (!row) return null
        this.db.prepare('DELETE FROM files WHERE id = ?').run(row.id)
        return deserializeChunks(row.chunks)
    }
}

function serializeChunks(chunks: ChunkRef[]): Buffer {
    const buf = Buffer.alloc(chunks.length * 4)
    for (let i = 0; i < chunks.length; i++) {
        buf.writeUInt16BE(chunks[i].bucket, i * 4)
        buf.writeUInt16BE(chunks[i].slot, i * 4 + 2)
    }
    return buf
}

function deserializeChunks(buf: Buffer): ChunkRef[] {
    const chunks: ChunkRef[] = []
    for (let i = 0; i < buf.length; i += 4) {
        chunks.push({ bucket: buf.readUInt16BE(i), slot: buf.readUInt16BE(i + 2) })
    }
    return chunks
}
