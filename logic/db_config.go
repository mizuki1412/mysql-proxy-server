package logic

type DBConfig struct {
	Targets []*Target `json:"targets"`
}

type Target struct {
	Port int           `json:"port"`
	Src  *TargetDetail `json:"src"`
	Dist *TargetDetail `json:"dist"`
}

type TargetDetail struct {
	Driver   string      `json:"driver"`
	Db       string      `json:"db"`
	Schema   string      `json:"schema"`
	Ip       string      `json:"ip"`
	Port     int         `json:"port"`
	Username string      `json:"username"`
	Password string      `json:"password"`
	Provider *DBProvider `json:"-"`
}

const (
	DriverMysql    = "mysql"
	DriverKingbase = "kingbase"
	DriverPostgres = "postgres"
)
