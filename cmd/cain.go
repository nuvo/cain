package main

import (
	"io"
	"log"
	"os"

	"github.com/maorfr/cain/pkg/cain"

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

	return cmd
}

type backupCmd struct {
	namespace string
	selector  string
	container string
	keyspace  string
	bucket    string
	parallel  int

	out io.Writer
}

// NewBackupCmd performs a backup of a cassandra cluster
func NewBackupCmd(out io.Writer) *cobra.Command {
	b := &backupCmd{out: out}

	cmd := &cobra.Command{
		Use:   "backup",
		Short: "backup cassandra cluster to S3",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			if err := cain.Backup(b.namespace, b.selector, b.container, b.keyspace, b.bucket, b.parallel); err != nil {
				log.Fatal(err)
			}
		},
	}
	f := cmd.Flags()

	f.StringVarP(&b.namespace, "namespace", "n", "", "namespace to find cassandra cluster")
	f.StringVarP(&b.selector, "selector", "l", "", "selector to filter on")
	f.StringVarP(&b.container, "container", "c", "cassandra", "container name to backup")
	f.StringVarP(&b.keyspace, "keyspace", "k", "", "keyspace to backup")
	f.StringVarP(&b.bucket, "bucket", "b", "", "bucket to backup to")
	f.IntVarP(&b.parallel, "parallel", "p", 1, "number of files to copy in parallel. set this flag to 0 for full parallelism")

	cmd.MarkFlagRequired("namespace")
	cmd.MarkFlagRequired("selector")
	cmd.MarkFlagRequired("keyspace")
	cmd.MarkFlagRequired("bucket")

	return cmd
}
