package config

import (
	"gopkg.in/yaml.v3"
	"log"
	"os"
)

type config struct {
	Proxyrack []struct {
		Host      string `yaml:"host"`
		PortStart int    `yaml:"port-start"`
		PortEnd   int    `yaml:"port-end"`
	}
	Debug struct {
		MongoUser     string `yaml:"mongo-user"`
		MongoPassword string `yaml:"mongo-password"`
		MongoDatabase string `yaml:"mongo-database"`
		GinHostPort   string `yaml:"gin-hostport"`
	}
	Prod struct {
		MongoUser     string `yaml:"mongo-user"`
		MongoPassword string `yaml:"mongo-password"`
		MongoDatabase string `yaml:"mongo-database"`
		GinHostPort   string `yaml:"gin-hostport"`
	}
	MongoCollection string `yaml:"mongo-collection"`
	MongoReplicaSet string `yaml:"mongo-replicaset"`
	MongoHosts      string `yaml:"mongo-hosts"`
	Scrapers        []struct {
		Scraper string `yaml:"name"`
	}
	UseProxyRack string `yaml:"useproxyrack"`
	Newproxies   struct {
		ResourceLink      string `yaml:"url"`
		ProxyListUsername string `yaml:"username"`
		ProxyListPassword string `yaml:"password"`
	}
	SchedulerTimings struct {
		LoadProxiesTime     uint64 `yaml:"loadProxies"`
		LogStatsTime        uint64 `yaml:"logStats"`
		ReturnPostponedTime uint64 `yaml:"returnPostponed"`
		SaveToMongoTime     uint64 `yaml:"saveToMongo"`
	}
	ProxyRelated struct {
		MaxGoodAttempts            int32 `yaml:"max-good-attempts"`
		BackoffTimeForGoodAttempts int64 `yaml:"backoff-time-for-good-attempts-attempts"`
		ProxyRackBackoffTime       int64 `yaml:"proxyrack-backoff-time"`
		RemoveDeadDays             int64 `yaml:"remove-dead-days"`
	}
	StatsFileName string `yaml:"stats-filename"`
}

var (
	yamlConfig config
	// ResourceLink is place for getting new proxies
	ResourceLink string
	// ProxyListUsername username for getting new proxies
	ProxyListUsername string
	// ProxyListPassword password for getting new proxies
	ProxyListPassword string
	// GinHostPort host & port for Gin server
	GinHostPort string
	// Scrapers list of scrapers
	Scrapers []string
	// UseProxyRack for use/not use Proxyrack proxies
	UseProxyRack  bool
	mongoUser     string
	mongoPassword string
	// MongoDatabase db in mongo
	MongoDatabase string
	// MongoCollection collection in mongo
	MongoCollection string
	mongoHosts      string
	// MongoURI link to mongo
	MongoURI string
	// ProxyrackProxyIP ip's of proxyrack proxies
	ProxyrackProxyIP []string
	// LoadProxiesTime interval to getting new proxies in seconds
	LoadProxiesTime uint64
	// LogStatsTime interval to console log output in seconds
	LogStatsTime uint64
	// ReturnPostponedTime interval to return proxies from non-working statuses in seconds
	ReturnPostponedTime uint64
	// SaveToMongoTime interval to save to mongo in seconds
	SaveToMongoTime uint64
	// MaxGoodAttempts shows how many good attempts can be passed before proxy postpone
	MaxGoodAttempts int32
	// ProxyrackBackoffTime time for proxyrack proxies backoff
	ProxyrackBackoffTime int64
	// BackoffTimeForGoodAttempts time for proxies backoff
	BackoffTimeForGoodAttempts int64
	// RemoveDeadTime after how many seconds dead proxies are deleted
	RemoveDeadTime int64
	// StatsFileName name for stats file
	StatsFileName string
)

// ParseConfig to parse config.yml file
func ParseConfig() {
	var configFile = "config.yml"
	data, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatal(err)
	}

	if err = yaml.Unmarshal(data, &yamlConfig); err != nil {
		log.Fatal(err)
	}
	ResourceLink = yamlConfig.Newproxies.ResourceLink
	ProxyListUsername = yamlConfig.Newproxies.ProxyListUsername
	ProxyListPassword = yamlConfig.Newproxies.ProxyListPassword
	if os.Getenv("pmserver") == "development" {
		mongoUser = yamlConfig.Debug.MongoUser
		mongoPassword = yamlConfig.Debug.MongoPassword
		MongoDatabase = yamlConfig.Debug.MongoDatabase
		GinHostPort = yamlConfig.Debug.GinHostPort
	} else {
		mongoUser = yamlConfig.Prod.MongoUser
		mongoPassword = yamlConfig.Prod.MongoPassword
		MongoDatabase = yamlConfig.Prod.MongoDatabase
		GinHostPort = yamlConfig.Prod.GinHostPort
	}
	MongoCollection = yamlConfig.MongoCollection
	mongoHosts = yamlConfig.MongoHosts + MongoDatabase + "?replicaSet=" + yamlConfig.MongoReplicaSet
	if os.Getenv("pmserver_local_run") == "local" {
		MongoURI = "mongodb://localhost:27017"
	} else {
		MongoURI = "mongodb://" + mongoUser + ":" + mongoPassword + "@" + mongoHosts + ""
	}
	for _, scraper := range yamlConfig.Scrapers {
		Scrapers = append(Scrapers, scraper.Scraper)
	}
	for _, record := range yamlConfig.Proxyrack {
		ProxyrackProxyIP = append(ProxyrackProxyIP, record.Host)
	}
	if yamlConfig.UseProxyRack == "no" {
		UseProxyRack = false
	} else {
		UseProxyRack = true
	}
	LoadProxiesTime = yamlConfig.SchedulerTimings.LoadProxiesTime
	LogStatsTime = yamlConfig.SchedulerTimings.LogStatsTime
	ReturnPostponedTime = yamlConfig.SchedulerTimings.ReturnPostponedTime
	SaveToMongoTime = yamlConfig.SchedulerTimings.SaveToMongoTime
	MaxGoodAttempts = yamlConfig.ProxyRelated.MaxGoodAttempts
	BackoffTimeForGoodAttempts = yamlConfig.ProxyRelated.BackoffTimeForGoodAttempts
	ProxyrackBackoffTime = yamlConfig.ProxyRelated.ProxyRackBackoffTime
	RemoveDeadTime = yamlConfig.ProxyRelated.RemoveDeadDays * 24 * 60 * 60
	StatsFileName = yamlConfig.StatsFileName
}
