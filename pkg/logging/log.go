package logging

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func SetupLogger() {
	// TODO: Make color configurable? Disabled so we don't have to deal with ANSI escape codes in our logoutput
	output := zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339, NoColor: true}
	output.FormatLevel = func(i interface{}) string {
		return strings.ToUpper(fmt.Sprintf("| %-6s|", i))
	}
	output.FormatMessage = func(i interface{}) string {
		return fmt.Sprintf("[ %s ]", i)
	}
	log.Logger = zerolog.New(output).With().Timestamp().Logger()
}

func GetLogger() zerolog.Logger {
	return log.Logger
}
