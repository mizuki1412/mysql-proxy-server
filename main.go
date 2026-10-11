package main

import (
	"mizuki/project/mysql-proxy-server/logic"

	"github.com/example/go-frame/pkg/class/exception"
	"github.com/example/go-frame/pkg/cli"
	"github.com/example/go-frame/pkg/library/filekit"
	"github.com/example/go-frame/pkg/library/jsonkit"
	"github.com/example/go-frame/pkg/service/configkit"
	_ "github.com/lib/pq"
	_ "github.com/mizuki1412/kingbase-go-driver"
	"github.com/spf13/cobra"
)

func main() {
	m := &cobra.Command{
		Use: "main",
		Run: func(cmd *cobra.Command, args []string) {
			fp := configkit.GetString("data_path")
			content, _ := filekit.ReadString(fp)
			if content == "" {
				panic(exception.New("配置文件内容不存在"))
			}
			err := jsonkit.ParseObj(content, logic.ConfigBean)
			if err != nil {
				panic(exception.New(err.Error()))
			}
			logic.Main()
		},
	}
	m.PersistentFlags().String("data_path", "./data.json", "")
	cli.RootCMD(m)
	cli.AddChildCMD(&cobra.Command{
		Use: "test",
		Run: func(cmd *cobra.Command, args []string) {
			logic.ClientExample()
		},
	})
	cli.Execute()
}
