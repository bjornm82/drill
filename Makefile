
.PHONY: bindata
bindata:
	go-bindata -o schema.go -pkg drill schema.json