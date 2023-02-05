module github.com/grammaton76/g76golib/pkg/sjson

go 1.18

require (
	github.com/go-ini/ini v1.67.0
	github.com/grammaton76/g76golib/pkg/slogger v0.0.0-00010101000000-000000000000
	github.com/shopspring/decimal v1.3.1
	golang.org/x/sys v0.0.0-20220919091848-fb04ddd9f9c8
)

require (
	github.com/kardianos/osext v0.0.0-20190222173326-2bc1f35cddc0 // indirect
	github.com/stretchr/testify v1.8.0 // indirect
)

replace github.com/grammaton76/g76golib/pkg/sjson => ../../../g76golib/pkg/sjson

replace github.com/grammaton76/g76golib/pkg/slogger => ../../../g76golib/pkg/slogger

replace github.com/grammaton76/g76golib/pkg/shared => ../../../g76golib/pkg/shared

replace github.com/grammaton76/cryptohandlers/pkg/okane => ../../pkg/okane

replace github.com/grammaton76/chattools/pkg/chat_output/sc_dbtable => ../../../chattools/pkg/chat_output/sc_dbtable
