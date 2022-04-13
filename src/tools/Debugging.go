package tools

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type LogTopic string

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	rootedPath, err := os.Getwd()
	if err != nil {
		log.Fatalf("%v\n", err)
	}
	fileName := rootedPath + "/" + "output.log"
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("%v\n", err)
	}
	log.SetOutput(file)

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic LogTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Milliseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}
