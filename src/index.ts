#!/usr/bin/env node
import { Binary, Types } from 'cafe-utility'
import { homedir } from 'node:os'
import { join, resolve } from 'node:path'
import { argv, env, loadEnvFile } from 'node:process'
import { benchSign, benchSplit, deleteFile, list, migrate, status, upload } from './commands.js'

main()

async function main() {
    try {
        loadEnvFile()
    } catch {
        // No .env file found, continue with environment variables
    }

    const command = Types.asString(argv[2])
    const stateDir = join(homedir(), '.etherchunk')

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
                `${Binary.uint8ArrayToHex(rootHash)}  ${path}  [${kind}]  ${chunkCount} chunks${redundancy}  uploaded=${uploaded}`
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
        throw new Error(`Unknown command: ${command}. Use status, list, upload, delete, migrate, bench:split, or bench:sign.`)
    }
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
