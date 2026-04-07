all: vtinsights

vtinsights: main.go go.mod go.sum
	go build -o vtinsights main.go
