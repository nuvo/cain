[![Release](https://img.shields.io/github/release/maorfr/cain.svg)](https://github.com/maorfr/cain/releases)
[![Travis branch](https://img.shields.io/travis/maorfr/cain/master.svg)](https://travis-ci.org/maorfr/cain)
[![Docker Pulls](https://img.shields.io/docker/pulls/maorfr/cain.svg)](https://hub.docker.com/r/maorfr/cain/)
[![Go Report Card](https://goreportcard.com/badge/github.com/maorfr/cain)](https://goreportcard.com/report/github.com/maorfr/cain)
[![license](https://img.shields.io/github/license/maorfr/cain.svg)](https://github.com/maorfr/cain/blob/master/LICENSE)

# Cain

Cain is a backup and restore tool for Cassandra on Kubernetes. It is named after the DC Comics superhero [Cassandra Cain](https://en.wikipedia.org/wiki/Cassandra_Cain).

Cain supports the following cloud storage services:

* AWS S3
* Minio S3
* Azure Blob Storage
* Google Cloud Storage

Cain is now an official part of the Helm [incubator/cassandra](https://github.com/helm/charts/tree/master/incubator/cassandra) chart!

## Install

### Prerequisites

1. git
2. [dep](https://github.com/golang/dep)

### From a release

Download the latest release from the [Releases page](https://github.com/maorfr/cain/releases) or use it with a [Docker image](https://hub.docker.com/r/maorfr/cain)

### From source

```shell
mkdir -p $GOPATH/src/github.com/maorfr && cd $_
git clone https://github.com/maorfr/cain.git && cd cain
make
```

## Commands

### Backup Cassandra cluster to cloud storage

Cain performs a backup in the following way:

1. Backup the `keyspace` schema (using `cqlsh`).
2. Get backup data using `nodetool snapshot` - it creates a snapshot of the `keyspace` in all Cassandra pods in the given `namespace` (according to `selector`).
3. Copy the files in `parallel` to cloud storage using [Skbn](https://github.com/maorfr/skbn) - it copies the files to the specified `dst`, under `namespace/<cassandrClusterName>/keyspace/<keyspaceSchemaHash>/tag/`.
4. Clear all snapshots.

#### Usage

```shell
$ cain backup --help
backup cassandra cluster to cloud storage

Usage:
  cain backup [flags]

Flags:
  -b, --buffer-size float           in memory buffer size (MB) to use for files copy (buffer per file). Overrides $CAIN_BUFFER_SIZE (default 6.75)
      --cassandra-data-dir string   cassandra data directory. Overrides $CAIN_CASSANDRA_DATA_DIR (default "/var/lib/cassandra/data")
  -c, --container string            container name to act on. Overrides $CAIN_CONTAINER (default "cassandra")
      --dst string                  destination to backup to. Example: s3://bucket/cassandra. Overrides $CAIN_DST
  -h, --help                        help for backup
  -k, --keyspace string             keyspace to act on. Overrides $CAIN_KEYSPACE
  -n, --namespace string            namespace to find cassandra cluster. Overrides $CAIN_NAMESPACE (default "default")
  -p, --parallel int                number of files to copy in parallel. set this flag to 0 for full parallelism. Overrides $CAIN_PARALLEL (default 1)
  -w, --password string             password for the cassandra connection. Overrides $CAIN_PASSWORD (default "cassandra")
  -l, --selector string             selector to filter on. Overrides $CAIN_SELECTOR (default "app=cassandra")
  -t, --tag string                  tag to backup, if empty then will use current timestamp. Use with cauthon - if tag exists then its contents will be overwritten. Overrides $CAIN_TAG
  -u, --username string             username for the cassandra connection. Overrides $CAIN_USERNAME (default "cassandra")

```

#### Examples

Backup to AWS S3

```shell
cain backup \
    -n default \
    -l release=cassandra \
    -k keyspace \
    --dst s3://db-backup/cassandra
```

Backup to AWS S3 using specific tag named `before-upgrade`

```shell
cain backup \
    -n default \
    -l release=cassandra \
    -k keyspace \
    --dst s3://db-backup/cassandra \
    -t before-upgrade
```

Backup to Azure Blob Storage

```shell
cain backup \
    -n default \
    -l release=cassandra \
    -k keyspace \
    --dst abs://my-account/db-backup-container/cassandra
```

Backup to Google Cloud Storage

```shell
cain backup \
    -n default \
    -l release=cassandra \
    -k keyspace \
    --dst gcs://db-backup/cassandra
```

### Restore Cassandra backup from cloud storage

Cain performs a restore in the following way:

1. Restore schema if `schema` is specified.
2. Truncate all tables in `keyspace`.
3. Copy files from the specified `src` (under `keyspace/<keyspaceSchemaHash>/tag/`) - restore is only possible for the same keyspace schema.
4. Load new data using `nodetool refresh`.

#### Usage

```shell
$ cain restore --help
restore cassandra cluster from cloud storage

Usage:
  cain restore [flags]

Flags:
  -b, --buffer-size float           in memory buffer size (MB) to use for files copy (buffer per file). Overrides $CAIN_BUFFER_SIZE (default 6.75)
      --cassandra-data-dir string   cassandra data directory. Overrides $CAIN_CASSANDRA_DATA_DIR (default "/var/lib/cassandra/data")
  -c, --container string            container name to act on. Overrides $CAIN_CONTAINER (default "cassandra")
  -k, --keyspace string             keyspace to act on. Overrides $CAIN_KEYSPACE
  -n, --namespace string            namespace to find cassandra cluster. Overrides $CAIN_NAMESPACE (default "default")
  -p, --parallel int                number of files to copy in parallel. set this flag to 0 for full parallelism. Overrides $CAIN_PARALLEL (default 1)
  -s, --schema string               schema version to restore (optional). Overrides $CAIN_SCHEMA
  -l, --selector string             selector to filter on. Overrides $CAIN_SELECTOR (default "app=cassandra")
      --src string                  source to restore from. Example: s3://bucket/cassandra/namespace/cluster-name. Overrides $CAIN_SRC
  -t, --tag string                  tag to restore. Overrides $CAIN_TAG
      --user-group string           user and group who should own restored files. Overrides $CAIN_USER_GROUP (default "cassandra:cassandra")
```

#### Examples

Restore from S3

```shell
cain restore \
    --src s3://db-backup/cassandra/default/ring01
    -n default \
    -k keyspace \
    -l release=cassandra \
    -t 20180903091624
```

Restore from S3 from specific tag used before

```shell
cain restore \
    --src s3://db-backup/cassandra/default/ring01
    -n default \
    -k keyspace \
    -l release=cassandra \
    -t before-upgrade
```

Restore from Azure Blob Storage

```shell
cain restore \
    --src s3://my-account/db-backup-container/cassandra/default/ring01
    -n default \
    -k keyspace \
    -l release=cassandra \
    -t 20180903091624
```

Restore from Google Cloud Storage

```shell
cain restore \
    --src gcs://db-backup/cassandra/default/ring01
    -n default \
    -k keyspace \
    -l release=cassandra \
    -t 20180903091624
```

### Describe keyspace schema

Cain describes the `keyspace` schema using `cqlsh`. It can return the schema itself, or a checksum of the schema file (used by `backup` and `restore`).

#### Usage

```shell
$ cain schema --help
get schema of cassandra cluster

Usage:
  cain schema [flags]

Flags:
  -c, --container string   container name to act on. Overrides $CAIN_CONTAINER (default "cassandra")
  -k, --keyspace string    keyspace to act on. Overrides $CAIN_KEYSPACE
  -n, --namespace string   namespace to find cassandra cluster. Overrides $CAIN_NAMESPACE (default "default")
  -l, --selector string    selector to filter on. Overrides $CAIN_SELECTOR (default "app=cassandra")
      --sum                print only checksum. Overrides $CAIN_SUM
```

#### Examples

```shell
cain schema \
    -n default \
    -l release=cassandra \
    -k keyspace
```

```shell
cain schema \
    -n default \
    -l release=cassandra \
    -k keyspace \
    --sum
```

## Environment variables support

Cain commands support the usage of environment variables instead of flags. For example:
The `backup` command can be executed as mentioned in the example:

```shell
cain backup \
    -n default \
    -l release=cassandra \
    -k keyspace \
    --dst s3://db-backup/cassandra
```

You can also set the appropriate envrionment variables (`CAIN_FLAG`, `_` instead of `-`):

```shell
export CAIN_NAMESPACE=default
export CAIN_SELECTOR=release=cassandra
export CAIN_KEYSPACE=keyspace
export CAIN_DST=s3://db-backup/cassandra

cain backup
```

## Support for additional storage services

Since Cain uses [Skbn](https://github.com/maorfr/skbn), adding support for additional storage services is simple. Read [this post](https://medium.com/maorfr-group-tech/copy-files-and-directories-between-kubernetes-and-s3-d290ded9a5e0) for more information.

## Skbn compatibility matrix

| Cain version | Skbn version |
|--------------|--------------|
| 0.6.0        | 0.5.0        |
| 0.5.1        | 0.4.2        |
| 0.5.0        | 0.4.1        |
| 0.4.2        | 0.4.1        |
| 0.4.1        | 0.4.1        |
| 0.4.0        | 0.4.0        |
| 0.3.0        | 0.3.0        |
| 0.2.0        | 0.2.0        |
| 0.1.0        | 0.1.1        |

## Credentials

### Kubernetes

Cain tries to get credentials in the following order:

1. if `KUBECONFIG` environment variable is set - cain will use the current context from that config file
2. if `~/.kube/config` exists - cain will use the current context from that config file with an [out-of-cluster client configuration](https://github.com/kubernetes/client-go/tree/master/examples/out-of-cluster-client-configuration)
3. if `~/.kube/config` does not exist - cain will assume it is working from inside a pod and will use an [in-cluster client configuration](https://github.com/kubernetes/client-go/tree/master/examples/in-cluster-client-configuration)

### AWS

Cain uses the default AWS [credentials chain](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html).

### Azure Blob Storage

Cain uses `AZURE_STORAGE_ACCOUNT` and `AZURE_STORAGE_ACCESS_KEY` environment variables for authentication.

### Google Cloud Storage

Cain uses Google [Application Default Credentials](https://cloud.google.com/docs/authentication/production). 
Basically, it will first look for the `GOOGLE_APPLICATION_CREDENTIALS` environment variable. If it is not defined, it will look for the default service account, or throw an error if none is configured. 

## Examples

1. [Helm example](/examples/helm)
2. [Code example](/examples/code)
