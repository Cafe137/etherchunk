# Postage Batch Setup

This guide explains how to obtain the three environment variables needed to use Swarm FS:
`SWARMFS_SIGNER`, `SWARMFS_BATCH_ID`, and `SWARMFS_BATCH_DEPTH`.

## Prerequisites

Install [swarm-cli](https://github.com/ethersphere/swarm-cli):

```sh
npm install --global @ethersphere/swarm-cli
```

## 1. Run a Bee node and buy a postage batch

Start a Bee node and purchase a postage batch using the Bee API or swarm-cli. The batch ID and depth returned by the purchase are what you need:

```sh
# Example: buy a batch with depth 22 and amount 10000000
swarm-cli stamp buy --depth 22 --amount 10000000
```

Use the returned values as:

```sh
export SWARMFS_BATCH_ID="<batch id hex from purchase>"
export SWARMFS_BATCH_DEPTH=<depth you specified>
```

## 2. Obtain the signer private key

Swarm FS signs stamps client-side and requires the private key that controls your postage batch. Export it from your Bee node's keystore using swarm-cli:

```sh
swarm-cli utility unlock <path to swarm.key>
```

This prints the raw private key hex. Use it as:

```sh
export SWARMFS_SIGNER="<private key hex>"
```

> **Keep this value secret.** Anyone with this key can sign stamps against your batch.
