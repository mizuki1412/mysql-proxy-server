package main

import (
	_ "github.com/lib/pq"
	"github.com/mizuki1412/go-core-kit/v2/cli"
	_ "github.com/mizuki1412/kingbase-go-driver"
	"github.com/spf13/cobra"
	"mizuki/project/mysql-proxy-server/logic"
)

func main() {
	m := &cobra.Command{
		Use: "main",
		Run: func(cmd *cobra.Command, args []string) {
			//fp := configkit.GetString("data_path")
			//content, _ := filekit.ReadString(fp)
			//if content == "" {
			//	panic(exception.New("配置文件内容不存在"))
			//}
			//err := jsonkit.ParseObj(content, logic.ConfigBean)
			//if err != nil {
			//	panic(exception.New(err.Error()))
			//}
			logic.Test()
		},
	}
	m.PersistentFlags().String("data_path", "./data.json", "")
	cli.RootCMD(m)
	cli.AddChildCMD(&cobra.Command{
		Use: "test",
		Run: func(cmd *cobra.Command, args []string) {

		},
	})
	cli.Execute()
}
