package main

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/jackc/pgx"
	"github.com/minio/go-homedir"
	"github.com/minio/minio-go"
	"net/http"
	"path/filepath"
	"os"
	"strconv"
	"log"
)

// Configuration file
type tomlConfig struct {
	Minio   minioInfo
	Pg      pgInfo
	DataGen dataGenInfo
	Web     webInfo
}

var conf tomlConfig

// Minio connection parameters
type minioInfo struct {
	Server    string
	AccessKey string `toml:"access_key"`
	Secret    string
	HTTPS     bool
}

// PostgreSQL connection parameters
type pgInfo struct {
	Server   string
	Port     int
	Username string
	Password string
	Database string
}

// Configuration info for the data generator
type dataGenInfo struct {
	Server         string
	HTTPS          bool
	Certificate    string
	CertificateKey string `toml:"certificate_key"`
}

// Configuration info just for us
type webInfo struct {
	Server         string
	Certificate    string
	CertificateKey string `toml:"certificate_key"`
}

var minioClient *minio.Client
var pgConfig = new(pgx.ConnConfig)

// Database connection
var db *pgx.Conn

func main() {
	// Read server configuration
	var err error
	if err = readConfig(); err != nil {
		log.Fatalf("Configuration file problem\n\n%v", err)
	}

	// Connect to Minio server
	minioClient, err = minio.New(conf.Minio.Server, conf.Minio.AccessKey, conf.Minio.Secret, conf.Minio.HTTPS)
	if err != nil {
		log.Fatalf("Problem with Minio server configuration: \n\n%v", err)
	}

	// Log Minio server end point
	log.Printf("Minio server config ok: %v\n", conf.Minio.Server)

	// Connect to PostgreSQL server
	db, err = pgx.Connect(*pgConfig)
	defer db.Close()
	if err != nil {
		log.Fatalf("Couldn't connect to database\n\n%v", err)
	}

	// Log successful connection message
	log.Printf("Connected to PostgreSQL server: %v:%v\n", conf.Pg.Server, uint16(conf.Pg.Port))

	// URL handlers
	http.HandleFunc("/", rootHandler)

	// Start server
	log.Printf("Starting DBHub webserver on https://%s\n", conf.Web.Server)
	log.Fatal(http.ListenAndServeTLS(conf.Web.Server, conf.Web.Certificate, conf.Web.CertificateKey, nil))
}

// Read the server configuration file
func readConfig() error {
	// Reads the server configuration from disk
	// TODO: Might be a good idea to add permission checks of the dir & conf file, to ensure they're not
	// TODO: world readable
	userHome, err := homedir.Dir()
	if err != nil {
		return fmt.Errorf("User home directory couldn't be determined: %s", "\n")
	}
	configFile := filepath.Join(userHome, ".dbhub", "config.toml")
	if _, err := toml.DecodeFile(configFile, &conf); err != nil {
		return fmt.Errorf("Config file couldn't be parsed: %v\n", err)
	}

	// Override config file via environment variables
	tempString := os.Getenv("MINIO_SERVER")
	if tempString != "" {
		conf.Minio.Server = tempString
	}
	tempString = os.Getenv("MINIO_ACCESS_KEY")
	if tempString != "" {
		conf.Minio.AccessKey = tempString
	}
	tempString = os.Getenv("MINIO_SECRET")
	if tempString != "" {
		conf.Minio.Secret = tempString
	}
	tempString = os.Getenv("MINIO_HTTPS")
	if tempString != "" {
		conf.Minio.HTTPS, err = strconv.ParseBool(tempString)
		if err != nil {
			return fmt.Errorf("Failed to parse MINIO_HTTPS: %v\n", err)
		}
	}
	tempString = os.Getenv("PG_SERVER")
	if tempString != "" {
		conf.Pg.Server = tempString
	}
	tempString = os.Getenv("PG_PORT")
	if tempString != "" {
		tempInt, err := strconv.ParseInt(tempString, 10, 0)
		if err != nil {
			return fmt.Errorf("Failed to parse PG_PORT: %v\n", err)
		}
		conf.Pg.Port = int(tempInt)
	}
	tempString = os.Getenv("PG_USER")
	if tempString != "" {
		conf.Pg.Username = tempString
	}
	tempString = os.Getenv("PG_PASS")
	if tempString != "" {
		conf.Pg.Password = tempString
	}
	tempString = os.Getenv("PG_DBNAME")
	if tempString != "" {
		conf.Pg.Database = tempString
	}

	// Verify we have the needed configuration information
	// Note - We don't check for a valid conf.Pg.Password here, as the PostgreSQL password can also be kept
	// in a .pgpass file as per https://www.postgresql.org/docs/current/static/libpq-pgpass.html
	var missingConfig []string
	if conf.Minio.Server == "" {
		missingConfig = append(missingConfig, "Minio server:port string")
	}
	if conf.Minio.AccessKey == "" {
		missingConfig = append(missingConfig, "Minio access key string")
	}
	if conf.Minio.Secret == "" {
		missingConfig = append(missingConfig, "Minio secret string")
	}
	if conf.Pg.Server == "" {
		missingConfig = append(missingConfig, "PostgreSQL server string")
	}
	if conf.Pg.Port == 0 {
		missingConfig = append(missingConfig, "PostgreSQL port number")
	}
	if conf.Pg.Username == "" {
		missingConfig = append(missingConfig, "PostgreSQL username string")
	}
	if conf.Pg.Password == "" {
		missingConfig = append(missingConfig, "PostgreSQL password string")
	}
	if conf.Pg.Database == "" {
		missingConfig = append(missingConfig, "PostgreSQL database string")
	}
	if len(missingConfig) > 0 {
		// Some config is missing
		returnMessage := fmt.Sprint("Missing or incomplete value(s):\n")
		for _, value := range missingConfig {
			returnMessage += fmt.Sprintf("\n \t→ %v", value)
		}
		return fmt.Errorf(returnMessage)
	}

	// Set the PostgreSQL configuration values
	pgConfig.Host = conf.Pg.Server
	pgConfig.Port = uint16(conf.Pg.Port)
	pgConfig.User = conf.Pg.Username
	pgConfig.Password = conf.Pg.Password
	pgConfig.Database = conf.Pg.Database
	pgConfig.TLSConfig = nil

	// The configuration file seems good
	return nil
}

func rootHandler(w http.ResponseWriter, req *http.Request) {

	// TODO: Everything :)

	fmt.Fprintln(w, "Stuff goes here")
}