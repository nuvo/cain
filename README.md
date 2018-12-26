[![Release](https://img.shields.io/github/release/nuvo/cain.svg)](https://github.com/nuvo/cain/releases)
[![Travis branch](https://img.shields.io/travis/nuvo/cain/master.svg)](https://travis-ci.org/nuvo/cain)
[![Docker Pulls](https://img.shields.io/docker/pulls/nuvo/cain.svg)](https://hub.docker.com/r/nuvo/cain/)
[![Go Report Card](https://goreportcard.com/badge/github.com/nuvo/cain)](https://goreportcard.com/report/github.com/nuvo/cain)
[![license](https://img.shields.io/github/license/nuvo/cain.svg)](https://github.com/nuvo/cain/blob/master/LICENSE)

# Cain

Cain is a backup and restore tool for Cassandra on Kubernetes. It is named after the DC Comics superhero [Cassandra Cain](https://en.wikipedia.org/wiki/Cassandra_Cain).

Cain supports the following cloud storage services:

* AWS S3
* Minio S3
* Azure Blob Storage

Cain is now an official part of the Helm [incubator/cassandra](https://github.com/helm/charts/tree/master/incubator/cassandra) chart!

## Install

### Prerequisites

1. git
2. [dep](https://github.com/golang/dep)

### From a release

Download the latest release from the [Releases page](https://github.com/nuvo/cain/releases) or use it with a [Docker image](https://hub.docker.com/r/nuvo/cain)

### From source

```
mkdir -p $GOPATH/src/github.com/nuvo && cd $_
git clone https://github.com/nuvo/cain.git && cd cain
make
```

## Commands

### Backup Cassandra cluster to cloud storage

Cain performs a backup in the following way:
1. Backup the `keyspace` schema (using `cqlsh`).
1. Get backup data using `nodetool snapshot` - it creates a snapshot of the `keyspace` in all Cassandra pods in the given `namespace` (according to `selector`).
2. Copy the files in `parallel` to cloud storage using [Skbn](https://github.com/nuvo/skbn) - it copies the files to the specified `dst`, under `namespace/<cassandrClusterName>/keyspace/<keyspaceSchemaHash>/tag/`.
3. Clear all snapshots.

#### Usage

```
$ cain backup --help
backup cassandra cluster to cloud storage

Usage:
  cain backup [flags]

Flags:
  -b, --buffer-size float   in memory buffer size (MB) to use for files copy (buffer per file) (default 6.75)
  -c, --container string    container name to act on (default "cassandra")
      --dst string          destination to backup to. Example: s3://bucket/cassandra
  -k, --keyspace string     keyspace to act on
  -n, --namespace string    namespace to find cassandra cluster
  -p, --parallel int        number of files to copy in parallel. set this flag to 0 for full parallelism (default 1)
  -l, --selector string     selector to filter on
```

#### Examples

Backup to AWS S3

```
cain backup \
    -n default \
    -l release=cassandra \
    -k keyspace \
    --dst s3://db-backup/cassandra
```

Backup to Azure Blob Storage

```
cain backup \
    -n default \
    -l release=cassandra \
    -k keyspace \
    --dst abs://my-account/db-backup-container/cassandra
```

### Restore Cassandra backup from cloud storage

Cain performs a restore in the following way:
1. Truncate all tables in `keyspace`.
2. Copy files from the specified `src` (under `keyspace/<keyspaceSchemaHash>/tag/`) - restore is only possible for the same keyspace schema.
3. Load new data using `nodetool refresh`.

* Cain does not currently restore the schema (must be loaded restoring).

#### Usage

```
$ cain restore --help
restore cassandra cluster from cloud storage

Usage:
  cain restore [flags]

Flags:
  -b, --buffer-size float   in memory buffer size (MB) to use for files copy (buffer per file) (default 6.75)
  -c, --container string    container name to act on (default "cassandra")
  -k, --keyspace string     keyspace to act on
  -n, --namespace string    namespace to find cassandra cluster
  -p, --parallel int        number of files to copy in parallel. set this flag to 0 for full parallelism (default 1)
  -s, --schema string       schema version to restore (optional)
  -l, --selector string     selector to filter on
      --src string          source to restore from. Example: s3://bucket/cassandra/namespace/cluster-name
  -t, --tag string          tag to restore
```

#### Examples

Restore from S3

```
cain restore \
    --src s3://db-backup/cassandra/default/ring01
    -n default \
    -k keyspace \
    -l release=cassandra \
    -t 20180903091624
```

Restore from Azure Blob Storage

```
cain restore \
    --src s3://my-account/db-backup-container/cassandra/default/ring01
    -n default \
    -k keyspace \
    -l release=cassandra \
    -t 20180903091624
```

### Describe keyspace schema

Cain describes the `keyspace` schema using `cqlsh`. It can return the schema itself, or a checksum of the schema file (used by `backup` and `restore`).

#### Usage

```
$ cain schema --help
get schema of cassandra cluster

Usage:
  cain schema [flags]

Flags:
  -c, --container string   container name to act on (default "cassandra")
  -h, --help               help for schema
  -k, --keyspace string    keyspace to act on
  -n, --namespace string   namespace to find cassandra cluster
  -l, --selector string    selector to filter on
      --sum                print only checksum
```

#### Examples

```
cain schema \
    -n default \
    -l release=cassandra \
    -k keyspace
```
```
cain schema \
    -n default \
    -l release=cassandra \
    -k keyspace \
    --sum
```

## Support for additional storage services

Since Cain uses [Skbn](https://github.com/nuvo/skbn), adding support for additional storage services is simple. Read [this post](https://medium.com/nuvo-group-tech/copy-files-and-directories-between-kubernetes-and-s3-d290ded9a5e0) for more information.

## Credentials


### Kubernetes

Cain tries to get credentials in the following order:
1. if `KUBECONFIG` environment variable is set - skbn will use the current context from that config file
2. if `~/.kube/config` exists - skbn will use the current context from that config file with an [out-of-cluster client configuration](https://github.com/kubernetes/client-go/tree/master/examples/out-of-cluster-client-configuration)
3. if `~/.kube/config` does not exist - skbn will assume it is working from inside a pod and will use an [in-cluster client configuration](https://github.com/kubernetes/client-go/tree/master/examples/in-cluster-client-configuration)


### AWS

Skbn uses the default AWS [credentials chain](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html).

### Azure Blob Storage

Skbn uses `AZURE_STORAGE_ACCOUNT` and `AZURE_STORAGE_ACCESS_KEY` environment variables for authentication.

## Examples

1. [Helm example](/examples/helm)
2. [Code example](/examples/code)
