package main

import (
	driver "olake-1/drivers/mysql"

	"github.com/datazip-inc/olake"
	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/protocol"
	_ "github.com/jackc/pgx/v4/stdlib"
)

func main() {
	driver := &driver.MySQL{
		Driver: base.NewBase(),
	}
	defer driver.Close()

	_ = protocol.ChangeStreamDriver(driver)
	olake.RegisterDriver(driver)
}
