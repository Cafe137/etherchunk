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
    const stateDir = join(homedir(), '.swarmfs')

    if (command === 'status') {
        const batchId = Binary.hexToUint8Array(Types.asHexString(env.SWARMFS_BATCH_ID))
        const batchDepth = Types.asNumber(env.SWARMFS_BATCH_DEPTH)
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
        const batchId = Binary.hexToUint8Array(Types.asHexString(env.SWARMFS_BATCH_ID))
        for (const { path, rootHash, kind, chunkCount, redundancyLevel, uploadDate } of list({ batchId, stateDir })) {
            const redundancy = redundancyLevel > 0 ? `  redundancy=${redundancyLevel}` : ''
            const uploaded = uploadDate ? new Date(uploadDate).toISOString() : 'unknown'
            console.log(
                `${Binary.uint8ArrayToHex(rootHash)}  ${path}  [${kind}]  ${chunkCount} chunks${redundancy}  uploaded=${uploaded}`
            )
        }
    } else if (command === 'upload') {
        const signer = Binary.uint256ToNumber(Binary.hexToUint8Array(Types.asHexString(env.SWARMFS_SIGNER)), 'BE')
        const batchId = Binary.hexToUint8Array(Types.asHexString(env.SWARMFS_BATCH_ID))
        const uploadUrl = Types.asString(env.SWARMFS_UPLOAD_URL)
        const batchDepth = Types.asNumber(env.SWARMFS_BATCH_DEPTH)
        const uploadArgs = argv.slice(3)
        const encrypt = uploadArgs.includes('--encrypt')
        const redundancyLevel =
            parseIntFlag(uploadArgs, '--redundancy') ?? parseInt(env.SWARMFS_REDUNDANCY_LEVEL ?? '0')
        const parallelism = parseIntFlag(uploadArgs, '--parallelism') ?? parseInt(env.SWARMFS_PARALLELISM ?? '32')
        const path = resolve(Types.asString(findPath(uploadArgs, '--redundancy', '--parallelism')))
        let lastFile = ''
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
            onProgress: (file, chunks) => {
                if (file !== lastFile) {
                    if (lastFile) process.stderr.write('\n')
                    lastFile = file
                }
                process.stderr.write(`\r  ${file} — ${chunks} chunks`)
            }
        })
        if (lastFile) process.stderr.write('\n')
        console.log(Binary.uint8ArrayToHex(rootHash))
    } else if (command === 'migrate') {
        const batchId = Binary.hexToUint8Array(Types.asHexString(env.SWARMFS_BATCH_ID))
        const batchDepth = Types.asNumber(env.SWARMFS_BATCH_DEPTH)
        const result = migrate({ batchId, batchDepth, stateDir })
        if (!result.migrated) {
            console.log(`Nothing to migrate: ${result.reason}`)
        } else {
            console.log(`Migrated slot map from depth ${result.oldDepth} to ${result.newDepth} (${result.oldSize} -> ${result.newSize} bytes)`)
            console.log(`Backup of the old state saved to ${result.backupPath}`)
        }
    } else if (command === 'delete') {
        const batchId = Binary.hexToUint8Array(Types.asHexString(env.SWARMFS_BATCH_ID))
        const batchDepth = Types.asNumber(env.SWARMFS_BATCH_DEPTH)
        const rootHash = Binary.hexToUint8Array(Types.asHexString(argv[3]))
        await deleteFile({ batchId, batchDepth, rootHash, stateDir })
    } else if (command === 'bench:split') {
        const benchArgs = argv.slice(3)
        const encrypt = benchArgs.includes('--encrypt')
        const redundancyLevel = parseIntFlag(benchArgs, '--redundancy') ?? parseInt(env.SWARMFS_REDUNDANCY_LEVEL ?? '0')
        const path = resolve(Types.asString(findPath(benchArgs, '--redundancy')))
        let lastFile = ''
        await benchSplit({
            path,
            encrypt,
            redundancyLevel,
            onProgress: (file, chunks) => {
                if (file !== lastFile) {
                    if (lastFile) process.stderr.write('\n')
                    lastFile = file
                }
                process.stderr.write(`\r  ${file} — ${chunks} chunks`)
            }
        })
        if (lastFile) process.stderr.write('\n')
    } else if (command === 'bench:sign') {
        const signer = Binary.uint256ToNumber(Binary.hexToUint8Array(Types.asHexString(env.SWARMFS_SIGNER)), 'BE')
        const batchId = Binary.hexToUint8Array(Types.asHexString(env.SWARMFS_BATCH_ID))
        const batchDepth = Types.asNumber(env.SWARMFS_BATCH_DEPTH)
        const benchArgs = argv.slice(3)
        const encrypt = benchArgs.includes('--encrypt')
        const redundancyLevel = parseIntFlag(benchArgs, '--redundancy') ?? parseInt(env.SWARMFS_REDUNDANCY_LEVEL ?? '0')
        const path = resolve(Types.asString(findPath(benchArgs, '--redundancy')))
        let lastFile = ''
        await benchSign({
            signer,
            batchId,
            batchDepth,
            path,
            encrypt,
            redundancyLevel,
            onProgress: (file, chunks) => {
                if (file !== lastFile) {
                    if (lastFile) process.stderr.write('\n')
                    lastFile = file
                }
                process.stderr.write(`\r  ${file} — ${chunks} chunks`)
            }
        })
        if (lastFile) process.stderr.write('\n')
    } else {
        throw new Error(`Unknown command: ${command}. Use status, list, upload, delete, migrate, bench:split, or bench:sign.`)
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
