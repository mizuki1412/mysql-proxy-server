package main

import (
	_ "github.com/lib/pq"
	"github.com/mizuki1412/go-core-kit/v2/cli"
	_ "github.com/mizuki1412/kingbase-go-driver"
	"github.com/spf13/cobra"
	"mizuki/project/mysql-proxy-server/test"
)

func main() {
	cli.RootCMD(&cobra.Command{
		Use: "main",
		Run: func(cmd *cobra.Command, args []string) {
			test.Main()
		},
	})
	cli.AddChildCMD(&cobra.Command{
		Use: "test",
		Run: func(cmd *cobra.Command, args []string) {

		},
	})
	cli.Execute()
}
