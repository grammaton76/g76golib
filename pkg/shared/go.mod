module github.com/grammaton76/g76golib/pkg/shared

go 1.16

require (
	github.com/VividCortex/mysqlerr v1.0.0
	github.com/fsnotify/fsnotify v1.4.7 // indirect
	github.com/go-ini/ini v1.67.0
	github.com/go-sql-driver/mysql v1.6.0
	github.com/grammaton76/g76golib/pkg/sjson v0.0.0-20221028045618-a4c734ae155b
	github.com/grammaton76/g76golib/pkg/slogger v0.0.0-20221028045618-a4c734ae155b
	github.com/kardianos/osext v0.0.0-20190222173326-2bc1f35cddc0
	github.com/lib/pq v1.10.6
	github.com/papertrail/go-tail v0.0.0-20180509224916-973c153b0431
	github.com/stretchr/testify v1.8.1 // indirect
	golang.org/x/sys v0.1.0 // indirect
)

replace github.com/grammaton76/g76golib/pkg/sjson => ../../../g76golib/pkg/sjson

replace github.com/grammaton76/g76golib/pkg/slogger => ../../../g76golib/pkg/slogger

replace github.com/grammaton76/g76golib/pkg/shared => ../../../g76golib/pkg/shared

replace github.com/grammaton76/chattools/pkg/chat_output/sc_dbtable => ../../../chattools/pkg/chat_output/sc_dbtable
