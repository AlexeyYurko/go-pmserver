package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-co-op/gocron"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/AlexeyYurko/go-pmserver/config"
	"github.com/AlexeyYurko/go-pmserver/db"
	"github.com/AlexeyYurko/go-pmserver/manager"
	stats "github.com/AlexeyYurko/go-pmserver/metrics"
	"github.com/AlexeyYurko/go-pmserver/now"
)

const (
	filePermissions = 0o666 // rw-rw-rw-
	hoursInDay      = 24
	minsInHour      = 60
	secsInMinute    = 60
)

func runSetup() {
	config.ParseConfig()
	db.Init()
	db.Load()
	reloadProxies()

	go executeCronJob()

	var pauseTime time.Duration = 500

	time.Sleep(pauseTime * time.Millisecond)
}

func main() {
	var logFile = "/tmp/proxymanager.log"
	file, err := os.OpenFile(logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, filePermissions)
	if err != nil {
		log.Error().Err(err).Msg("Could not open log file")
	}

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	multi := zerolog.MultiLevelWriter(os.Stdout, file)
	log.Logger = zerolog.New(multi).With().Timestamp().Logger()

	runSetup()

	router := setupRouter()
	err = router.Run(config.GinHostPort)
	if err != nil {
		log.Fatal().Err(err).Msg("Troubles with Gin Server")
	}
}

func checkErrCron(err error, schedulerName string, timing uint64) {
	if err != nil {
		log.Fatal().Err(err).Msg("Troubles with setting scheduler")
	}

	log.Info().Msgf("schedule planned for %s function with period of %d seconds.", schedulerName, timing)
}

func executeCronJob() {
	scheduler := gocron.NewScheduler(time.UTC)
	_, err := scheduler.Every(config.LoadProxiesTime).Seconds().Do(reloadProxies)
	checkErrCron(err, "reloadProxies", config.LoadProxiesTime)
	_, err = scheduler.Every(config.LogStatsTime).Seconds().Do(stats.LogStats)
	checkErrCron(err, "logStats", config.LogStatsTime)
	_, err = scheduler.Every(config.ReturnPostponedTime).Seconds().Do(returnPostponedWithCondition)
	checkErrCron(err, "returnPostponedWithCondition", config.ReturnPostponedTime)
	_, err = scheduler.Every(config.SaveToMongoTime).Seconds().Do(db.Save)
	checkErrCron(err, "saveToMongo", config.SaveToMongoTime)
	scheduler.StartAsync()
}

func setupRouter() *gin.Engine {
	router := gin.Default()
	router.Use(stats.RequestStats())
	router.GET("/", indexPage)
	router.GET("/get-random", routeGetRandom)
	router.GET("/inc-good-attempts", routeIncGoodAttempts)
	router.GET("/mark-dead", routeMarkDead)
	router.GET("/reanimate", routeReanimate)
	router.GET("/start", start)
	router.GET("/alive-from-dead", routeAliveFromDead)
	router.GET("/remove-days", removeDays)
	router.GET("/max-good-attempts", changeMaxGoodAttempts)
	router.GET("/remove-dead", removeDead)
	router.GET("/reload-proxy-list", reloadProxyList)
	router.GET("/get-working-list", getWorkingList)
	router.GET("/get-dead-list", getDeadList)
	router.GET("/get-proxy-usefulness-stats", getProxyUsefulnessStats)
	router.GET("/clear-usefulness-stats", routeClearUsefulnessStats)
	router.GET("/zstats", showStatsForZabbix)
	router.GET("/hstats", showHTMLStats)
	router.GET("/stats", metrics)
	router.POST("/add-proxies", routeAddProxies)
	router.POST("/remove-proxies", routeRemoveProxies)

	return router
}

func metrics(c *gin.Context) {
	c.JSON(http.StatusOK, stats.Report())
}

func indexPage(c *gin.Context) {
	c.String(http.StatusOK, "PMServer - GO version - Hello")
}

func routeGetRandom(context *gin.Context) {
	scraper := context.Query("scraper")

	if scraper == "" {
		context.String(http.StatusForbidden, "Field scraper is empty")

		return
	}

	randomProxy, err := manager.GetRandomProxy(scraper)

	if err != nil {
		context.String(http.StatusNoContent, "")
	} else {
		context.String(http.StatusOK, randomProxy)
	}
}

func routeIncGoodAttempts(context *gin.Context) {
	scraper := context.Query("scraper")
	proxy := context.Query("proxy")

	if (scraper == "") || (proxy == "") {
		context.String(http.StatusForbidden, "Field scraper or proxy is empty")

		return
	}

	manager.IncGoodAttempts(scraper, proxy)
	context.String(http.StatusOK, "OK")
}

func routeMarkDead(context *gin.Context) {
	scraper := context.Query("scraper")
	proxy := context.Query("proxy")

	if (scraper == "") || (proxy == "") {
		context.String(http.StatusForbidden, "Field scraper or proxy is empty")

		return
	}

	manager.MarkDead(scraper, proxy)
	context.String(http.StatusOK, "OK")
}

func routeReanimate(context *gin.Context) {
	scraper := context.Query("scraper")
	if scraper == "" {
		context.String(http.StatusForbidden, "Field scraper is empty")

		return
	}

	manager.ReanimateProxies(scraper)
	context.String(http.StatusOK, "OK")
}

func routeAliveFromDead(context *gin.Context) {
	scraper := context.Query("scraper")
	if scraper == "" {
		context.String(http.StatusForbidden, "Field scraper is empty")

		return
	}

	db.Base.AliveFromDead(scraper)
	context.String(http.StatusOK, "OK")
}

func removeDays(context *gin.Context) {
	days := context.Query("days")
	if days == "" {
		context.String(http.StatusForbidden, "Field days is empty")

		return
	}

	daysInt64, err := strconv.ParseInt(days, 10, 64)
	if err == nil {
		log.Info().Msgf("New clearance time set to %d days.\n", daysInt64)
		config.RemoveDeadTime = daysInt64 * hoursInDay * minsInHour * secsInMinute
	}

	context.String(http.StatusOK, "OK")
}

func changeMaxGoodAttempts(context *gin.Context) {
	numbers := context.Query("numbers")
	if numbers == "" {
		context.String(http.StatusForbidden, "Field numbers is empty")

		return
	}

	maxAttempts, err := strconv.ParseInt(numbers, 10, 32)

	if err == nil {
		log.Info().Msgf("New max attempts value set to %d.\n", maxAttempts)
		config.MaxGoodAttempts = int32(maxAttempts)
	}

	context.String(http.StatusOK, "OK")
}

func removeDead(context *gin.Context) {
	scraper := context.Query("scraper")
	if scraper == "" {
		context.String(http.StatusForbidden, "Field scraper is empty")

		return
	}

	manager.RemoveDeadProxiesForALongTime(scraper)

	context.String(http.StatusOK, "OK")
}

func reloadProxyList(c *gin.Context) {
	reloadProxies()
	c.String(http.StatusOK, "OK")
}

func start(c *gin.Context) {
	scraper := c.Query("scraper")
	log.Info().Msgf("%s\n", scraper)
}

func getWorkingList(context *gin.Context) {
	scraper := context.Query("scraper")
	if scraper == "" {
		context.String(http.StatusForbidden, "Field scraper is empty")

		return
	}

	workingList := db.Set.GetWorking(scraper)
	sort.Strings(workingList)
	outputLines := strings.Join(workingList, "\n")
	context.String(http.StatusOK, outputLines)
}

func getDeadList(context *gin.Context) {
	scraper := context.Query("scraper")
	if scraper == "" {
		context.String(http.StatusForbidden, "Field scraper is empty")

		return
	}

	deadList := db.Set.GetDead(scraper)
	sort.Strings(deadList)
	outputLines := strings.Join(deadList, "\n")
	context.String(http.StatusOK, outputLines)
}

func getProxyUsefulnessStats(context *gin.Context) {
	var name string

	var statsOrders = []string{"name", "sdate", "success", "fdate", "fail"}

	scraper := context.Query("scraper")
	orderBy := context.DefaultQuery("order_by", "name")
	found := find(statsOrders, orderBy)

	if scraper == "" || !found {
		context.String(http.StatusForbidden, "Field scraper is empty or wrong orderBy")

		return
	}

	stats.ProxyUsefulnessStatsToCSV(scraper, orderBy)

	saveTime := stats.UnixTimeString(now.Time())

	name = scraper + "_" + saveTime + "_" + config.StatsFileName
	context.FileAttachment(config.StatsFileName, name)
	context.String(http.StatusOK, "OK")
}

func routeClearUsefulnessStats(c *gin.Context) {
	db.Base.ClearUsefulnessStats()
	c.String(http.StatusOK, "OK")
}

func showStatsForZabbix(c *gin.Context) {
	c.String(http.StatusOK, "OK")
}

func showHTMLStats(c *gin.Context) {
	page := stats.HTMLStats()
	bytePage := []byte(page)
	c.Data(http.StatusOK, "text/html; charset=utf-8", bytePage)
}

// routeAddProxies for adding list of proxies to scraper
// format {"scraper": <name>, "proxies": [list_of_proxies]}
// if the field "scraper" is not specified, the proxy list applies to all scrapers.
func routeAddProxies(context *gin.Context) {
	var json struct {
		Proxies []string `json:"proxies,omitempty"`
		Scraper string   `json:"scraper,omitempty"`
	}

	err := context.BindJSON(&json)

	if err != nil {
		context.String(http.StatusForbidden, "Something went wrong")

		return
	}

	if len(json.Proxies) == 0 {
		context.String(http.StatusForbidden, "Empty proxies list")

		return
	}

	db.StoreProxies(json.Scraper, json.Proxies)

	context.String(http.StatusOK, "OK")
}

// routeRemoveProxies for adding list of proxies to scraper
// format {"scraper": <name>, "proxies": [list_of_proxies]}
// if the field "scraper" is not specified, the proxy list applies to all scrapers.
func routeRemoveProxies(context *gin.Context) {
	var json struct {
		Proxies []string `json:"proxies,omitempty"`
		Scraper string   `json:"scraper,omitempty"`
	}

	err := context.BindJSON(&json)

	if err != nil {
		context.String(http.StatusForbidden, "Something went wrong")

		return
	}

	if len(json.Proxies) == 0 {
		context.String(http.StatusForbidden, "Empty proxies list")

		return
	}

	db.Base.RemoveProxies(json.Scraper, json.Proxies)

	context.String(http.StatusOK, "OK")
}

func reloadProxies() {
	var proxies []string

	var scraperToAdd = ""

	log.Info().Msg("Reloading Proxies")

	proxies, err := loadProxiesFromWeb(config.ResourceLink)
	if err != nil {
		log.Error().Err(err).Msg("Error loading proxies")
	}

	db.StoreProxies(scraperToAdd, proxies)
}

func loadProxiesFromWeb(resourceLink string) ([]string, error) {
	ctx := context.Background()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, resourceLink, http.NoBody)

	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	req.SetBasicAuth(config.ProxyListUsername, config.ProxyListPassword)
	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)

	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	proxies := strings.Split(string(body), "\n")

	return proxies, nil
}

func returnPostponedWithCondition() {
	for _, scraper := range config.Scrapers {
		sizeOfBusyAndPostponed := db.Set.BusyAndPostponedSize(scraper)
		if sizeOfBusyAndPostponed == 0 {
			return
		}
		busyNotReturnedForMoreThan5Minutes := stats.GetStatsForTimeWithinBusyPostponed(scraper)["max"] < (-5 * 60)
		percentOfAvailableProxiesRest := float64(db.Set.AvailableSize(scraper)) / float64(sizeOfBusyAndPostponed)
		lessThan5PercentOfAvailableProxiesRest := percentOfAvailableProxiesRest*100 < 5.0
		percent := 100.0
		log.Info().Msgf("[%s] percent of available proxies from busy and postponed %.2f%%\n", scraper, percentOfAvailableProxiesRest*percent)

		if lessThan5PercentOfAvailableProxiesRest || busyNotReturnedForMoreThan5Minutes {
			manager.ReanimateProxies(scraper)
		}
	}
}

// find takes a slice and looks for an element in it. If found it will
// return a bool of true.
func find(slice []string, val string) bool {
	for _, item := range slice {
		if strings.Contains(val, item) {
			return true
		}
	}

	return false
}
