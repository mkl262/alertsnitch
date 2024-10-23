package main

import (
	"flag"
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"gitlab.com/yakshaving.art/alertsnitch/pkg/env"

	"github.com/sirupsen/logrus"

	"gitlab.com/yakshaving.art/alertsnitch/internal"
	"gitlab.com/yakshaving.art/alertsnitch/internal/db"
	"gitlab.com/yakshaving.art/alertsnitch/internal/server"
	"gitlab.com/yakshaving.art/alertsnitch/version"
)

// Args are the arguments that can be passed to alertsnitch
type Args struct {
	Address                string
	DBBackend              string
	DSN                    string
	MaxIdleConns           int
	MaxOpenConns           int
	MaxConnLifetimeSeconds int

	LokiTenantID           string
	LokiBasicAuthUser      string
	LokiBasicAuthPassword  string

	Debug   bool
	DryRun  bool
	Version bool
}

func main() {
	if err := godotenv.Load(); err != nil {
		logrus.Warn("Error loading .env file")
	}

	args := Args{}

	flag.BoolVar(&args.Version, "version", false, "print the version and exit")
	flag.StringVar(&args.Address, "listen.address", env.GetEnv("ALERTSNITCH_ADDR", ":9567"), "address in which to listen for http requests")
	flag.BoolVar(&args.Debug, "debug", env.GetEnvAsBool("ALERTSNITCH_DEBUG", false), "enable debug mode, which dumps alerts payloads to the log as they arrive")

	flag.StringVar(&args.DBBackend, "database-backend", env.GetEnv("ALERTSNITCH_BACKEND", "mysql"), "database backend, allowed are mysql, postgres, loki, and null")
	flag.StringVar(&args.DSN, "dsn", env.GetEnv(internal.DSNVar, ""), "Database DSN")

	flag.IntVar(&args.MaxOpenConns, "max-open-connections", env.GetEnvAsInt("ALERTSNITCH_MAX_OPEN_CONNS", 2), "maximum number of connections in the pool")
	flag.IntVar(&args.MaxIdleConns, "max-idle-connections", env.GetEnvAsInt("ALERTSNITCH_MAX_IDLE_CONNS", 1), "maximum number of idle connections in the pool")
	flag.IntVar(&args.MaxConnLifetimeSeconds, "max-connection-lifetyme-seconds", env.GetEnvAsInt("ALERTSNITCH_MAX_CONN_LIFETIME", 600), "maximum number of seconds a connection is kept alive in the pool")

	flag.StringVar(&args.LokiTenantID, "tenant-id", env.GetEnv("ALERTSNITCH_LOKI_TENANT_ID", ""), "Loki tenant ID")
	flag.StringVar(&args.LokiBasicAuthUser, "basic-auth-user", env.GetEnv("ALERTSNITCH_LOKI_BASIC_AUTH_USER", ""), "Loki basic auth user")
	flag.StringVar(&args.LokiBasicAuthPassword, "basic-auth-password", env.GetEnv("ALERTSNITCH_LOKI_BASIC_AUTH_PASSWORD", ""), "Loki basic auth password")

	flag.Parse()

	if args.Version {
		fmt.Println(version.GetVersion())
		os.Exit(0)
	}

	if args.Debug {
		logrus.SetLevel(logrus.DebugLevel)
	}
	driver, err := db.Connect(args.DBBackend, db.ConnectionArgs{
		DSN:                    args.DSN,
		MaxIdleConns:           args.MaxIdleConns,
		MaxOpenConns:           args.MaxOpenConns,
		MaxConnLifetimeSeconds: args.MaxConnLifetimeSeconds,

		Options: map[string]string{
			"tenant_id":           args.LokiTenantID,
			"basic_auth_user":     args.LokiBasicAuthUser,
			"basic_auth_password": args.LokiBasicAuthPassword,
		},
	})
	if err != nil {
		fmt.Println("failed to connect to database:", err)
		os.Exit(1)
	}

	fmt.Println("Connected to database")

	s := server.New(driver, args.Debug)
	s.Start(args.Address)
}
