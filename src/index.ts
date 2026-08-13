#!/usr/bin/env node
import { Binary, Types } from 'cafe-utility'
import { homedir } from 'node:os'
import { join, resolve } from 'node:path'
import { argv, env, loadEnvFile } from 'node:process'
import { benchSign, benchSplit, deleteFile, exportAll, list, migrate, status, upload } from './commands.js'

main()

async function main() {
    try {
        loadEnvFile()
    } catch {
        // No .env file found, continue with environment variables
    }

    const command = argv[2]
    const stateDir = join(homedir(), '.etherchunk')

    if (!command) {
        printHelp()
        return
    }

    if (command === 'status') {
        const batchId = Binary.hexToUint8Array(Types.asHexString(env.ETHERCHUNK_BATCH_ID))
        const batchDepth = Types.asNumber(env.ETHERCHUNK_BATCH_DEPTH)
        const { totalSlots, occupiedSlots, freeSlots, slotsPerBucket, mostUtilizedBucket, mostUtilizedCount } = status({
            batchId,
            batchDepth,
            stateDir
        })
        console.log(`Slots: ${occupiedSlots} occupied, ${freeSlots} free, ${totalSlots} total`)
        console.log(
            `Most utilized bucket: 0x${mostUtilizedBucket
                .toString(16)
                .padStart(4, '0')} (${mostUtilizedCount}/${slotsPerBucket} slots occupied)`
        )
    } else if (command === 'list') {
        const batchId = Binary.hexToUint8Array(Types.asHexString(env.ETHERCHUNK_BATCH_ID))
        for (const { path, rootHash, kind, chunkCount, redundancyLevel, uploadDate } of list({ batchId, stateDir })) {
            const redundancy = redundancyLevel > 0 ? `  redundancy=${redundancyLevel}` : ''
            const uploaded = uploadDate ? new Date(uploadDate).toISOString() : 'unknown'
            console.log(
                `${maskHash(Binary.uint8ArrayToHex(rootHash))}  ${maskPath(path)}  [${kind}]  ${chunkCount} chunks${redundancy}  uploaded=${uploaded}`
            )
        }
    } else if (command === 'upload') {
        const signer = Binary.uint256ToNumber(Binary.hexToUint8Array(Types.asHexString(env.ETHERCHUNK_SIGNER)), 'BE')
        const batchId = Binary.hexToUint8Array(Types.asHexString(env.ETHERCHUNK_BATCH_ID))
        const uploadUrl = Types.asString(env.ETHERCHUNK_UPLOAD_URL)
        const batchDepth = Types.asNumber(env.ETHERCHUNK_BATCH_DEPTH)
        const uploadArgs = argv.slice(3)
        const encrypt = uploadArgs.includes('--encrypt')
        const redundancyLevel =
            parseIntFlag(uploadArgs, '--redundancy') ?? parseInt(env.ETHERCHUNK_REDUNDANCY_LEVEL ?? '0')
        const parallelism = parseIntFlag(uploadArgs, '--parallelism') ?? parseInt(env.ETHERCHUNK_PARALLELISM ?? '32')
        const path = resolve(Types.asString(findPath(uploadArgs, '--redundancy', '--parallelism')))
        const progress = makeProgressReporter()
        const rootHash = await upload({
            signer,
            batchId,
            uploadUrl,
            batchDepth,
            path,
            stateDir,
            encrypt,
            redundancyLevel,
            parallelism,
            onProgress: progress.onProgress
        })
        progress.done()
        console.log(Binary.uint8ArrayToHex(rootHash))
    } else if (command === 'export') {
        const batchId = Binary.hexToUint8Array(Types.asHexString(env.ETHERCHUNK_BATCH_ID))
        const exportArgs = argv.slice(3)
        const parallelism = parseIntFlag(exportArgs, '--parallelism') ?? parseInt(env.ETHERCHUNK_PARALLELISM ?? '32')
        // Downloads are GET /chunks/<address> on the same endpoint uploads POST to, so the
        // upload URL already points at the right place; ETHERCHUNK_DOWNLOAD_URL is only needed
        // to read the content back from a different node or gateway than it was pushed to.
        const downloadUrl = env.ETHERCHUNK_DOWNLOAD_URL ?? Types.asString(env.ETHERCHUNK_UPLOAD_URL)
        const outDir = resolve(findPath(exportArgs, '--parallelism') ?? 'etherchunk-export')
        const results = await exportAll({
            batchId,
            stateDir,
            outDir,
            downloadUrl,
            parallelism,
            onProgress: (_reference, path, bytes) => process.stderr.write(`  ${maskPath(path)} — ${bytes} bytes\n`)
        })
        if (results.length === 0) {
            console.log('Nothing to export — no uploads are tracked for this batch.')
        }
        for (const { rootHash, directory, files, recovered, error } of results) {
            const hash = maskHash(Binary.uint8ArrayToHex(rootHash))
            if (error) {
                console.log(`${hash}  FAILED: ${error}`)
            } else {
                const bytes = files.reduce((total, file) => total + file.bytes, 0)
                console.log(`${hash}  ${files.length} file(s), ${bytes} bytes  ->  ${maskPath(directory)}`)
            }
            // A repaired chunk means the content survived but the upload is decaying on the
            // network, which is worth saying out loud even when the export succeeded.
            const repairs = [
                recovered.parity > 0 ? `${recovered.parity} from parity` : null,
                recovered.replica > 0 ? `${recovered.replica} from dispersed replicas` : null
            ].filter(Boolean)
            if (repairs.length > 0) {
                console.log(`${' '.repeat(hash.length)}  recovered ${repairs.join(', ')}`)
            }
        }
        if (results.some(result => result.error)) {
            process.exitCode = 1
        }
    } else if (command === 'migrate') {
        const batchId = Binary.hexToUint8Array(Types.asHexString(env.ETHERCHUNK_BATCH_ID))
        const batchDepth = Types.asNumber(env.ETHERCHUNK_BATCH_DEPTH)
        const result = migrate({ batchId, batchDepth, stateDir })
        if (!result.migrated) {
            console.log(`Nothing to migrate: ${result.reason}`)
        } else {
            console.log(`Migrated slot map from depth ${result.oldDepth} to ${result.newDepth} (${result.oldSize} -> ${result.newSize} bytes)`)
            console.log(`Backup of the old state saved to ${result.backupPath}`)
        }
    } else if (command === 'delete') {
        const batchId = Binary.hexToUint8Array(Types.asHexString(env.ETHERCHUNK_BATCH_ID))
        const batchDepth = Types.asNumber(env.ETHERCHUNK_BATCH_DEPTH)
        const rootHash = Binary.hexToUint8Array(Types.asHexString(argv[3]))
        await deleteFile({ batchId, batchDepth, rootHash, stateDir })
    } else if (command === 'bench:split') {
        const benchArgs = argv.slice(3)
        const encrypt = benchArgs.includes('--encrypt')
        const redundancyLevel = parseIntFlag(benchArgs, '--redundancy') ?? parseInt(env.ETHERCHUNK_REDUNDANCY_LEVEL ?? '0')
        const path = resolve(Types.asString(findPath(benchArgs, '--redundancy')))
        const progress = makeProgressReporter()
        await benchSplit({
            path,
            encrypt,
            redundancyLevel,
            onProgress: progress.onProgress
        })
        progress.done()
    } else if (command === 'bench:sign') {
        const signer = Binary.uint256ToNumber(Binary.hexToUint8Array(Types.asHexString(env.ETHERCHUNK_SIGNER)), 'BE')
        const batchId = Binary.hexToUint8Array(Types.asHexString(env.ETHERCHUNK_BATCH_ID))
        const batchDepth = Types.asNumber(env.ETHERCHUNK_BATCH_DEPTH)
        const benchArgs = argv.slice(3)
        const encrypt = benchArgs.includes('--encrypt')
        const redundancyLevel = parseIntFlag(benchArgs, '--redundancy') ?? parseInt(env.ETHERCHUNK_REDUNDANCY_LEVEL ?? '0')
        const path = resolve(Types.asString(findPath(benchArgs, '--redundancy')))
        const progress = makeProgressReporter()
        await benchSign({
            signer,
            batchId,
            batchDepth,
            path,
            encrypt,
            redundancyLevel,
            onProgress: progress.onProgress
        })
        progress.done()
    } else {
        throw new Error(`Unknown command: ${command}. Use status, list, upload, export, delete, migrate, bench:split, or bench:sign.`)
    }
}

function printHelp() {
    console.log(`etherchunk — client-side chunk stamping and slot tracking on top of Bee

Usage: etherchunk <command> [options]

Commands:
  upload <file|dir>    Upload a file or directory and print the manifest root hash
                       Options: --encrypt, --redundancy=<0-4>, --parallelism=<n>
  list                 List all tracked files and manifests
  export [dir]         Download every tracked upload into <dir>/<root hash>/ (default: ./etherchunk-export)
                       Options: --parallelism=<n>
  delete <root hash>   Delete a file or manifest by root hash, reclaiming its slots
  status               Show slot usage and most utilized bucket
  migrate              Migrate the slot map after a batch depth change
  bench:split <file|dir>  Benchmark chunk splitting speed (no upload, no state changes)
  bench:sign <file|dir>   Benchmark chunk splitting + stamp signing speed (no upload)

Configuration is read from environment variables (or a local .env file):
  ETHERCHUNK_UPLOAD_URL, ETHERCHUNK_SIGNER, ETHERCHUNK_BATCH_ID, ETHERCHUNK_BATCH_DEPTH,
  ETHERCHUNK_REDUNDANCY_LEVEL, ETHERCHUNK_PARALLELISM, ETHERCHUNK_PRIVATE,
  ETHERCHUNK_DOWNLOAD_URL (defaults to ETHERCHUNK_UPLOAD_URL)`)
}

// When ETHERCHUNK_PRIVATE=true, redact filenames and hashes from all output so
// they can't be read off a shared screen. Off by default.
function isPrivate(): boolean {
    return (env.ETHERCHUNK_PRIVATE ?? 'false').toLowerCase() === 'true'
}

// ab...ef -> ab***ef, keeping only the first and last two hex chars.
function maskHash(hex: string): string {
    if (!isPrivate()) return hex
    if (hex.length <= 4) return '*'.repeat(hex.length)
    return `${hex.slice(0, 2)}***${hex.slice(-2)}`
}

// Replace the basename of a path with ***, preserving the directory and any
// bracketed status markers like "(manifest)".
function maskPath(path: string): string {
    if (!isPrivate()) return path
    if (path.startsWith('(')) return path
    const idx = Math.max(path.lastIndexOf('/'), path.lastIndexOf('\\'))
    return idx >= 0 ? `${path.slice(0, idx + 1)}***` : '***'
}

function makeProgressReporter(): { onProgress: (file: string, chunks: number) => void; done: () => void } {
    const isTty = Boolean(process.stderr.isTTY)
    let lastFile = ''
    return {
        onProgress: (file, chunks) => {
            if (file !== lastFile) {
                if (lastFile && isTty) process.stderr.write('\n')
                lastFile = file
                // Without a TTY, carriage-return overwrites don't work (e.g. GitHub
                // Actions), so emit a single line per file instead of one per chunk.
                if (!isTty) process.stderr.write(`  ${file}\n`)
            }
            if (isTty) process.stderr.write(`\r  ${file} — ${chunks} chunks`)
        },
        done: () => {
            if (lastFile && isTty) process.stderr.write('\n')
        }
    }
}

function parseIntFlag(args: string[], flag: string): number | undefined {
    const eqForm = args.find(a => a.startsWith(`${flag}=`))
    if (eqForm) return parseInt(eqForm.slice(flag.length + 1))
    const idx = args.indexOf(flag)
    if (idx >= 0 && idx + 1 < args.length) return parseInt(args[idx + 1])
    return undefined
}

function findPath(args: string[], ...intFlags: string[]): string | undefined {
    const skipNext = new Set<number>()
    for (const flag of intFlags) {
        const idx = args.indexOf(flag)
        if (idx >= 0) skipNext.add(idx + 1)
    }
    return args.find((a, i) => !a.startsWith('--') && !skipNext.has(i))
}
