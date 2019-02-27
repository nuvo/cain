package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/nuvo/cain/pkg/cain"
	"github.com/nuvo/cain/pkg/utils"
	"github.com/spf13/cobra"
)

func main() {
	cmd := NewRootCmd(os.Args[1:])
	if err := cmd.Execute(); err != nil {
		log.Fatal("Failed to execute command")
	}
}

// NewRootCmd represents the base command when called without any subcommands
func NewRootCmd(args []string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cain",
		Short: "",
		Long:  ``,
	}

	out := cmd.OutOrStdout()

	cmd.AddCommand(NewBackupCmd(out))
	cmd.AddCommand(NewIncBackupCmd(out))
	cmd.AddCommand(NewRestoreCmd(out))
	cmd.AddCommand(NewSchemaCmd(out))
	cmd.AddCommand(NewVersionCmd(out))

	return cmd
}

type backupCmd struct {
	namespace        string
	selector         string
	container        string
	keyspace         string
	dst              string
	parallel         int
	bufferSize       float64
	cassandraDataDir string

	out io.Writer
}

// NewBackupCmd performs a backup of a cassandra cluster
func NewBackupCmd(out io.Writer) *cobra.Command {
	b := &backupCmd{out: out}

	cmd := &cobra.Command{
		Use:   "backup",
		Short: "backup cassandra cluster to cloud storage",
		Long:  ``,
		Args: func(cmd *cobra.Command, args []string) error {
			if b.dst == "" {
				return errors.New("dst can not be empty")
			}
			if b.keyspace == "" {
				return errors.New("keyspace can not be empty")
			}
			if strings.HasSuffix(strings.TrimRight(b.dst, "/"), b.keyspace) {
				log.Println("WARNING: Destination path should not include the name of the keyspace")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			options := cain.BackupOptions{
				Namespace:        b.namespace,
				Selector:         b.selector,
				Container:        b.container,
				Keyspace:         b.keyspace,
				Dst:              b.dst,
				Parallel:         b.parallel,
				BufferSize:       b.bufferSize,
				CassandraDataDir: b.cassandraDataDir,
			}
			if _, err := cain.Backup(options); err != nil {
				log.Fatal(err)
			}
		},
	}
	f := cmd.Flags()

	f.StringVarP(&b.namespace, "namespace", "n", utils.GetStringEnvVar("CAIN_NAMESPACE", "default"), "namespace to find cassandra cluster. Overrides $CAIN_NAMESPACE")
	f.StringVarP(&b.selector, "selector", "l", utils.GetStringEnvVar("CAIN_SELECTOR", "app=cassandra"), "selector to filter on. Overrides $CAIN_SELECTOR")
	f.StringVarP(&b.container, "container", "c", utils.GetStringEnvVar("CAIN_CONTAINER", "cassandra"), "container name to act on. Overrides $CAIN_CONTAINER")
	f.StringVarP(&b.keyspace, "keyspace", "k", utils.GetStringEnvVar("CAIN_KEYSPACE", ""), "keyspace to act on. Overrides $CAIN_KEYSPACE")
	f.StringVar(&b.dst, "dst", utils.GetStringEnvVar("CAIN_DST", ""), "destination to backup to. Example: s3://bucket/cassandra. Overrides $CAIN_DST")
	f.IntVarP(&b.parallel, "parallel", "p", utils.GetIntEnvVar("CAIN_PARALLEL", 1), "number of files to copy in parallel. set this flag to 0 for full parallelism. Overrides $CAIN_PARALLEL")
	f.Float64VarP(&b.bufferSize, "buffer-size", "b", utils.GetFloat64EnvVar("CAIN_BUFFER_SIZE", 6.75), "in memory buffer size (MB) to use for files copy (buffer per file). Overrides $CAIN_BUFFER_SIZE")
	f.StringVar(&b.cassandraDataDir, "cassandra-data-dir", utils.GetStringEnvVar("CAIN_CASSANDRA_DATA_DIR", "/var/lib/cassandra/data"), "cassandra data directory. Overrides $CAIN_CASSANDRA_DATA_DIR")

	return cmd
}

type incBackupCmd struct {
	namespace        string
	selector         string
	container        string
	keyspace         string
	dst              string
	parallel         int
	bufferSize       float64
	cassandraDataDir string

	out io.Writer
}

// NewIncBackupCmd performs a copy of incremental backup folders to cloud storage
func NewIncBackupCmd(out io.Writer) *cobra.Command {
	b := &incBackupCmd{out: out}

	cmd := &cobra.Command{
		Use:   "incremental",
		Short: "backup cassandra cluster's incremental backup folders to cloud storage",
		Long:  ``,
		Args: func(cmd *cobra.Command, args []string) error {
			if b.dst == "" {
				return errors.New("dst can not be empty")
			}
			if b.keyspace == "" {
				return errors.New("keyspace can not be empty")
			}
			if strings.HasSuffix(strings.TrimRight(b.dst, "/"), b.keyspace) {
				log.Println("WARNING: Destination path should not include the name of the keyspace")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			options := cain.IncBackupOptions{
				Namespace:        b.namespace,
				Selector:         b.selector,
				Container:        b.container,
				Keyspace:         b.keyspace,
				Dst:              b.dst,
				Parallel:         b.parallel,
				BufferSize:       b.bufferSize,
				CassandraDataDir: b.cassandraDataDir,
			}
			if _, err := cain.IncBackup(options); err != nil {
				log.Fatal(err)
			}
		},
	}
	f := cmd.Flags()

	f.StringVarP(&b.namespace, "namespace", "n", utils.GetStringEnvVar("CAIN_NAMESPACE", "default"), "namespace to find cassandra cluster. Overrides $CAIN_NAMESPACE")
	f.StringVarP(&b.selector, "selector", "l", utils.GetStringEnvVar("CAIN_SELECTOR", "app=cassandra"), "selector to filter on. Overrides $CAIN_SELECTOR")
	f.StringVarP(&b.container, "container", "c", utils.GetStringEnvVar("CAIN_CONTAINER", "cassandra"), "container name to act on. Overrides $CAIN_CONTAINER")
	f.StringVarP(&b.keyspace, "keyspace", "k", utils.GetStringEnvVar("CAIN_KEYSPACE", ""), "keyspace to act on. Overrides $CAIN_KEYSPACE")
	f.StringVar(&b.dst, "dst", utils.GetStringEnvVar("CAIN_DST", ""), "destination to backup to. Example: s3://bucket/cassandra. Overrides $CAIN_DST")
	f.IntVarP(&b.parallel, "parallel", "p", utils.GetIntEnvVar("CAIN_PARALLEL", 1), "number of files to copy in parallel. set this flag to 0 for full parallelism. Overrides $CAIN_PARALLEL")
	f.Float64VarP(&b.bufferSize, "buffer-size", "b", utils.GetFloat64EnvVar("CAIN_BUFFER_SIZE", 6.75), "in memory buffer size (MB) to use for files copy (buffer per file). Overrides $CAIN_BUFFER_SIZE")
	f.StringVar(&b.cassandraDataDir, "cassandra-data-dir", utils.GetStringEnvVar("CAIN_CASSANDRA_DATA_DIR", "/var/lib/cassandra/data"), "cassandra data directory. Overrides $CAIN_CASSANDRA_DATA_DIR")

	return cmd
}

type restoreCmd struct {
	src              string
	keyspace         string
	tag              string
	schema           string
	namespace        string
	selector         string
	container        string
	parallel         int
	bufferSize       float64
	userGroup        string
	cassandraDataDir string

	out io.Writer
}

// NewRestoreCmd performs a restore from backup of a cassandra cluster
func NewRestoreCmd(out io.Writer) *cobra.Command {
	r := &restoreCmd{out: out}

	cmd := &cobra.Command{
		Use:   "restore",
		Short: "restore cassandra cluster from cloud storage",
		Long:  ``,
		Args: func(cmd *cobra.Command, args []string) error {
			if r.src == "" {
				return errors.New("src can not be empty")
			}
			if r.tag == "" {
				return errors.New("tag can not be empty")
			}
			if r.keyspace == "" {
				return errors.New("keyspace can not be empty")
			}
			if strings.HasSuffix(strings.TrimRight(r.src, "/"), r.keyspace) {
				log.Println("WARNING: Source path should not include the name of the keyspace")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			options := cain.RestoreOptions{
				Src:              r.src,
				Keyspace:         r.keyspace,
				Tag:              r.tag,
				Schema:           r.schema,
				Namespace:        r.namespace,
				Selector:         r.selector,
				Container:        r.container,
				Parallel:         r.parallel,
				BufferSize:       r.bufferSize,
				UserGroup:        r.userGroup,
				CassandraDataDir: r.cassandraDataDir,
			}
			if err := cain.Restore(options); err != nil {
				log.Fatal(err)
			}
		},
	}
	f := cmd.Flags()

	f.StringVar(&r.src, "src", utils.GetStringEnvVar("CAIN_SRC", ""), "source to restore from. Example: s3://bucket/cassandra/namespace/cluster-name. Overrides $CAIN_SRC")
	f.StringVarP(&r.keyspace, "keyspace", "k", utils.GetStringEnvVar("CAIN_KEYSPACE", ""), "keyspace to act on. Overrides $CAIN_KEYSPACE")
	f.StringVarP(&r.tag, "tag", "t", utils.GetStringEnvVar("CAIN_TAG", ""), "tag to restore. Overrides $CAIN_TAG")
	f.StringVarP(&r.schema, "schema", "s", utils.GetStringEnvVar("CAIN_SCHEMA", ""), "schema version to restore (optional). Overrides $CAIN_SCHEMA")
	f.StringVarP(&r.namespace, "namespace", "n", utils.GetStringEnvVar("CAIN_NAMESPACE", "default"), "namespace to find cassandra cluster. Overrides $CAIN_NAMESPACE")
	f.StringVarP(&r.selector, "selector", "l", utils.GetStringEnvVar("CAIN_SELECTOR", "app=cassandra"), "selector to filter on. Overrides $CAIN_SELECTOR")
	f.StringVarP(&r.container, "container", "c", utils.GetStringEnvVar("CAIN_CONTAINER", "cassandra"), "container name to act on. Overrides $CAIN_CONTAINER")
	f.IntVarP(&r.parallel, "parallel", "p", utils.GetIntEnvVar("CAIN_PARALLEL", 1), "number of files to copy in parallel. set this flag to 0 for full parallelism. Overrides $CAIN_PARALLEL")
	f.Float64VarP(&r.bufferSize, "buffer-size", "b", utils.GetFloat64EnvVar("CAIN_BUFFER_SIZE", 6.75), "in memory buffer size (MB) to use for files copy (buffer per file). Overrides $CAIN_BUFFER_SIZE")
	f.StringVar(&r.userGroup, "user-group", utils.GetStringEnvVar("CAIN_USER_GROUP", "cassandra:cassandra"), "user and group who should own restored files. Overrides $CAIN_USER_GROUP")
	f.StringVar(&r.cassandraDataDir, "cassandra-data-dir", utils.GetStringEnvVar("CAIN_CASSANDRA_DATA_DIR", "/var/lib/cassandra/data"), "cassandra data directory. Overrides $CAIN_CASSANDRA_DATA_DIR")

	return cmd
}

type schemaCmd struct {
	namespace string
	selector  string
	container string
	keyspace  string
	sum       bool

	out io.Writer
}

// NewSchemaCmd performs schema related actions
func NewSchemaCmd(out io.Writer) *cobra.Command {
	s := &schemaCmd{out: out}

	cmd := &cobra.Command{
		Use:   "schema",
		Short: "get schema of cassandra cluster",
		Long:  ``,
		Args: func(cmd *cobra.Command, args []string) error {
			if s.keyspace == "" {
				return errors.New("keyspace can not be empty")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			options := cain.SchemaOptions{
				Namespace: s.namespace,
				Selector:  s.selector,
				Container: s.container,
				Keyspace:  s.keyspace,
			}
			schema, sum, err := cain.Schema(options)
			if err != nil {
				log.Fatal(err)
			}

			if s.sum {
				fmt.Println(sum)
			} else {
				fmt.Println((string)(schema))
			}
		},
	}
	f := cmd.Flags()

	f.StringVarP(&s.namespace, "namespace", "n", utils.GetStringEnvVar("CAIN_NAMESPACE", "default"), "namespace to find cassandra cluster. Overrides $CAIN_NAMESPACE")
	f.StringVarP(&s.selector, "selector", "l", utils.GetStringEnvVar("CAIN_SELECTOR", "app=cassandra"), "selector to filter on. Overrides $CAIN_SELECTOR")
	f.StringVarP(&s.container, "container", "c", utils.GetStringEnvVar("CAIN_CONTAINER", "cassandra"), "container name to act on. Overrides $CAIN_CONTAINER")
	f.StringVarP(&s.keyspace, "keyspace", "k", utils.GetStringEnvVar("CAIN_KEYSPACE", ""), "keyspace to act on. Overrides $CAIN_KEYSPACE")
	f.BoolVar(&s.sum, "sum", utils.GetBoolEnvVar("CAIN_SUM", false), "print only checksum. Overrides $CAIN_SUM")

	return cmd
}

var (
	// GitTag stands for a git tag
	GitTag string
	// GitCommit stands for a git commit hash
	GitCommit string
)

// NewVersionCmd prints version information
func NewVersionCmd(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("Version %s (git-%s)\n", GitTag, GitCommit)
		},
	}

	return cmd
}
