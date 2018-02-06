package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"gopkg.in/ini.v1"

	"database/sql"
	"github.com/go-sql-driver/mysql"
)

func readDebianMySQLConfig(path string) (map[string]string, error) {

	cfg, err := ini.LoadSources(ini.LoadOptions{
		Insensitive:         true,
		IgnoreInlineComment: true,
		AllowBooleanKeys:    true},
		path)
	if err != nil {
		return map[string]string{}, err
	}
	cfg.BlockMode = false

	clientSection := cfg.Section("client")
	if clientSection == nil {
		return map[string]string{}, errors.New("failed to get mysql client configuration")
	}

	if !clientSection.HasKey("host") ||
		!clientSection.HasKey("user") ||
		!clientSection.HasKey("password") ||
		!clientSection.HasKey("socket") {
		return map[string]string{}, errors.New("failed to get mysql client configuration")
	}

	return clientSection.KeysHash(), nil
}

func genDsn(cfgMap map[string]string) string {

	cfg := mysql.NewConfig()
	cfg.User = cfgMap["user"]
	cfg.Passwd = cfgMap["password"]
	cfg.Net = "unix"
	cfg.Addr = cfgMap["socket"]

	return cfg.FormatDSN()
}

func getMyISAMTables(dsn string) ([]string, error) {

	var err error
	var tables []string

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return []string{}, err
	}

	defer func() {
		err := db.Close()
		if err != nil {
			log.Printf("failed to defer: %s\n", err.Error())
		}
	}()

	err = db.Ping()
	if err != nil {
		return []string{}, err
	}

	sql := `SELECT
            CONCAT(TABLE_SCHEMA, '.', TABLE_NAME)
          FROM information_schema.TABLES
          WHERE TABLE_TYPE='BASE TABLE'
            AND TABLE_SCHEMA
                NOT IN ('mysql', 'performance_schema')
            AND ENGINE='MyISAM';`

	rows, err := db.Query(sql)
	if err != nil {
		return []string{}, err
	}

	defer func() {
		err := rows.Close()
		if err != nil {
			log.Printf("failed to defer: %s\n", err.Error())
		}
	}()

	for rows.Next() {

		var table string

		err = rows.Scan(&table)
		if err != nil {
			return []string{}, err
		}

		tables = append(tables, table)
	}

	if err := rows.Err(); err != nil {
		return []string{}, err
	}

	return tables, nil
}

func lockMyISAMTables(dsn string, tables []string, socket string) error {

	var err error

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}

	err = db.Ping()
	if err != nil {
		return err
	}

	// to see locks on DB.TABLES issue this SQL: SHOW OPEN TABLES WHERE In_use > 0;
	sql := fmt.Sprintf("FLUSH TABLES %s WITH READ LOCK;", strings.Join(tables, ", "))

	_, err = db.Exec(sql)
	if err != nil {
		return err
	}

	l, err := listenOnUnixSocket(socket)
	if err != nil {
		return err
	}

	defer func() {
		err := db.Close()
		if err != nil {
			log.Printf("failed to defer: %s\n", err.Error())
		}
	}()

	defer func() {
		err := os.Remove(socket)
		if err != nil {
			log.Printf("failed to defer: %s\n", err.Error())
		}
	}()

	for {
		time.Sleep(100 * time.Millisecond)
		err = db.Ping()
		if err != nil {
			log.Println(err.Error())
		}

		buf, err := readUnixSocket(l)
		if err != nil {
			log.Println(err.Error())
			break
		}

		if strings.TrimSpace(buf) == "UNLOCK_MYISAM_TABLES" {
			break
		}
	}

	_, err = db.Exec("UNLOCK TABLES;")
	if err != nil {
		log.Println(err.Error())
	}

	return nil
}

func unLockMyISAMTables(socket string) error {

	var err error

	laddr := net.UnixAddr{Name: socket, Net: "unix"}

	c, err := net.DialUnix("unix", &laddr, &net.UnixAddr{Name: socket, Net: "unix"})
	if err != nil {
		return err
	}

	defer func() {
		err := c.Close()
		if err != nil {
			log.Printf("failed to defer: %s\n", err.Error())
		}
	}()

	for i := 0; i < 10; i++ {
		time.Sleep(50 * time.Millisecond)
		_, err = c.Write([]byte("UNLOCK_MYISAM_TABLES\n"))
		if err != nil {
			return err
		}
	}

	return nil
}

func listenOnUnixSocket(socket string) (*net.UnixListener, error) {
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
	if err != nil {
		return nil, err
	}

	return l, nil
}

func readUnixSocket(l *net.UnixListener) (string, error) {
	c, err := l.AcceptUnix()
	if err != nil {
		return "", err
	}

	defer func() {
		err := c.Close()
		if err != nil {
			log.Printf("failed to defer: %s\n", err.Error())
		}
	}()

	var buf [1024]byte

	n, err := c.Read(buf[:])
	if err != nil {
		return "", err
	}

	return string(buf[:n]), nil
}

func main() {

	unixSocketPath := flag.String("unix-socket-path", "/var/run/mysqld/backup.sock", "unix socket path to use for communication")
	debianMysqlConfigPath := flag.String("mysql-config-path", "/etc/mysql/debian.cnf", "path to MySQL configuration file")

	lockPtr := flag.Bool("lock-tables", false, "issue lock to all MyISAM tables")
	unlockPtr := flag.Bool("unlock-tables", false, "issue unlock to all tables")

	flag.Parse()

	if *lockPtr {

		err := os.Remove(*unixSocketPath)
		if err != nil {
			log.Fatalln(err.Error())
		}

		cfgMap, err := readDebianMySQLConfig(*debianMysqlConfigPath)
		if err != nil {
			log.Fatalln(err.Error())
		}

		dsn := genDsn(cfgMap)

		tables, err := getMyISAMTables(dsn)
		if err != nil {
			log.Fatalln(err.Error())
		}

		err = lockMyISAMTables(dsn, tables, *unixSocketPath)
		if err != nil {
			log.Fatalln(err.Error())
		}
	}

	if *unlockPtr {
		err := unLockMyISAMTables(*unixSocketPath)
		if err != nil {
			log.Fatalln(err.Error())
		}

		defer func() {
			err := os.Remove(*unixSocketPath)
			if err != nil {
				log.Printf("failed to defer: %s\n", err.Error())
			}
		}()
	}

	if !*unlockPtr && !*lockPtr {
		fmt.Printf("Use %s -h to see all options\n", os.Args[0])
		os.Exit(0)
	}
}
