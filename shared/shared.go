package shared

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

func GetChatHandleAsUser(db *DbHandle, user string) *Stmt {
	var Sql string
	switch db.dbtype {
	case DbTypePostgres:
		Sql = fmt.Sprintf("INSERT INTO chat_messages (handle, channel, status, message, written) VALUES ('%s', $1, 'PENDING', $2, NOW());", user)
	case DbTypeMysql:
		Sql = fmt.Sprintf("INSERT INTO chat_messages (handle, channel, status, message, written) VALUES ('%s', ?, 'PENDING', ?, NOW());", user)
	}
	Chat := db.Prepare(Sql).OrDie("chatasuser prepare")
	return Chat
}
