package main

import (
	"dfs/file_server/server"
	"log"
)

func main() {
	if err := server.StartServer(); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
