# Cain

Cain is a Backup/Restore tool for Cassandra on Kubernetes. It is named after the DC Comics superhero [Cassandra Cain](https://en.wikipedia.org/wiki/Cassandra_Cain).


## Build from source

Cain uses [glide](https://github.com/Masterminds/glide) as a dependency management tool, since some of the referenced packages are not available using [dep](https://github.com/golang/dep).

```
glide up
go build -o cain cmd/cain.go
```


## Commands

### Backup Cassandra cluster to S3

Cain performs a backup in the following way:
1. Backup the `keyspace` schema (using `cqlsh`) and copy it to S3.
1. Get backup data using `nodetool snapshot` - it creates a snapshot of the `keyspace` in all Cassandra pods in the given `namespace` (according to `selector`).
2. Copy the files in `parallel` to S3 using [Skbn](https://github.com/maorfr/skbn) - it copies the files to the specified `dst`, under `namespace/<cassandrClusterName>/keyspace/<keyspaceSchemaHash>/tag/`.
3. Clear all snapshots.

#### Usage

```
$ cain backup --help
backup cassandra cluster to S3

Usage:
  cain backup [flags]

Flags:
  -c, --container string   container name to act on (default "cassandra")
      --dst string         destination to backup to. Example: s3://bucket/cassandra
  -h, --help               help for backup
  -k, --keyspace string    keyspace to act on
  -n, --namespace string   namespace to find cassandra cluster
  -p, --parallel int       number of files to copy in parallel. set this flag to 0 for full parallelism (default 1)
  -l, --selector string    selector to filter on
```

#### Example

```
cain backup \
    -n default \
    -l release=cassandra \
    -k keyspace \
    --dst s3://db-backup/cassandra \
    -p 0
```

### Restore Cassandra backup from S3

Cain performs a restore in the following way:
1. Truncate all tables in `keyspace`.
2. Copy files from the specified `src` (under `keyspace/<keyspaceSchemaHash>/tag/`) - restore is only possible for the same keyspace schema.
3. Load new data using `nodetool refresh`.

* Cain does not currently restore the schema (must be loaded restoring).

#### Usage

```
$ cain restore --help
restore cassandra cluster from S3

Usage:
  cain restore [flags]

Flags:
  -c, --container string   container name to act on (default "cassandra")
  -h, --help               help for restore
  -k, --keyspace string    keyspace to act on
  -n, --namespace string   namespace to find cassandra cluster
  -p, --parallel int       number of files to copy in parallel. set this flag to 0 for full parallelism (default 1)
  -l, --selector string    selector to filter on
      --src string         source to restore from. Example: s3://bucket/cassandra/namespace/cluster-name
  -t, --tag string         tag to restore
```

#### Example

```
cain restore \
    --src s3://db-backup/cassandra/default/ring01
    -n default \
    -k keyspace \
    -l release=cassandra \
    -t 20180903091624 \
    -p 0
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

#### Example

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

Since Cain uses [Skbn](https://github.com/maorfr/skbn), adding support for additional storage services is simple. Read [this post](https://medium.com/nuvo-group-tech/copy-files-and-directories-between-kubernetes-and-s3-d290ded9a5e0) for more information.

## Credentials


### Kubernetes

Cain tries to get credentials in the following order:
1. if `KUBECONFIG` environment variable is set - skbn will use the current context from that config file
2. if `~/.kube/config` exists - skbn will use the current context from that config file with an [out-of-cluster client configuration](https://github.com/kubernetes/client-go/tree/master/examples/out-of-cluster-client-configuration)
3. if `~/.kube/config` does not exist - skbn will assume it is working from inside a pod and will use an [in-cluster client configuration](https://github.com/kubernetes/client-go/tree/master/examples/in-cluster-client-configuration)


### AWS

Skbn uses the default AWS [credentials chain](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html).
