package main

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/nuvo/cain/pkg/cain"

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
	cmd.AddCommand(NewRestoreCmd(out))
	cmd.AddCommand(NewSchemaCmd(out))
	cmd.AddCommand(NewVersionCmd(out))

	return cmd
}

type backupCmd struct {
	namespace string
	selector  string
	container string
	keyspace  string
	dst       string
	parallel  int

	out io.Writer
}

// NewBackupCmd performs a backup of a cassandra cluster
func NewBackupCmd(out io.Writer) *cobra.Command {
	b := &backupCmd{out: out}

	cmd := &cobra.Command{
		Use:   "backup",
		Short: "backup cassandra cluster to cloud storage",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			if _, err := cain.Backup(b.namespace, b.selector, b.container, b.keyspace, b.dst, b.parallel); err != nil {
				log.Fatal(err)
			}
		},
	}
	f := cmd.Flags()

	f.StringVarP(&b.namespace, "namespace", "n", "", "namespace to find cassandra cluster")
	f.StringVarP(&b.selector, "selector", "l", "", "selector to filter on")
	f.StringVarP(&b.container, "container", "c", "cassandra", "container name to act on")
	f.StringVarP(&b.keyspace, "keyspace", "k", "", "keyspace to act on")
	f.StringVar(&b.dst, "dst", "", "destination to backup to. Example: s3://bucket/cassandra")
	f.IntVarP(&b.parallel, "parallel", "p", 1, "number of files to copy in parallel. set this flag to 0 for full parallelism")

	return cmd
}

type restoreCmd struct {
	src       string
	keyspace  string
	tag       string
	namespace string
	selector  string
	container string
	parallel  int

	out io.Writer
}

// NewRestoreCmd performs a restore from backup of a cassandra cluster
func NewRestoreCmd(out io.Writer) *cobra.Command {
	r := &restoreCmd{out: out}

	cmd := &cobra.Command{
		Use:   "restore",
		Short: "restore cassandra cluster from cloud storage",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			if err := cain.Restore(r.src, r.keyspace, r.tag, r.namespace, r.selector, r.container, r.parallel); err != nil {
				log.Fatal(err)
			}
		},
	}
	f := cmd.Flags()

	f.StringVar(&r.src, "src", "", "source to restore from. Example: s3://bucket/cassandra/namespace/cluster-name")
	f.StringVarP(&r.keyspace, "keyspace", "k", "", "keyspace to act on")
	f.StringVarP(&r.tag, "tag", "t", "", "tag to restore")
	f.StringVarP(&r.namespace, "namespace", "n", "", "namespace to find cassandra cluster")
	f.StringVarP(&r.selector, "selector", "l", "", "selector to filter on")
	f.StringVarP(&r.container, "container", "c", "cassandra", "container name to act on")
	f.IntVarP(&r.parallel, "parallel", "p", 1, "number of files to copy in parallel. set this flag to 0 for full parallelism")

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
		Run: func(cmd *cobra.Command, args []string) {
			schema, sum, err := cain.Schema(s.namespace, s.selector, s.container, s.keyspace)
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

	f.StringVarP(&s.namespace, "namespace", "n", "", "namespace to find cassandra cluster")
	f.StringVarP(&s.selector, "selector", "l", "", "selector to filter on")
	f.StringVarP(&s.container, "container", "c", "cassandra", "container name to act on")
	f.StringVarP(&s.keyspace, "keyspace", "k", "", "keyspace to act on")
	f.BoolVar(&s.sum, "sum", false, "print only checksum")

	cmd.MarkFlagRequired("namespace")
	cmd.MarkFlagRequired("selector")

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
