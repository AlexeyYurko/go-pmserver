package main

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/AlexeyYurko/go-pmserver/db"
	"github.com/AlexeyYurko/go-pmserver/manager"
	"github.com/AlexeyYurko/go-pmserver/now"
	"github.com/go-co-op/gocron"
	"github.com/hashicorp/logutils"

	"github.com/AlexeyYurko/go-pmserver/config"
	stats "github.com/AlexeyYurko/go-pmserver/metrics"
	"github.com/gin-gonic/gin"
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
	var file, err = os.OpenFile(logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("Could Not Open Log File : " + err.Error())
	}
	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"DEBUG", "WARN", "ERROR", "INFO"},
		MinLevel: logutils.LogLevel("DEBUG"),
		Writer:   io.MultiWriter(file, os.Stdout),
	}
	log.SetOutput(filter)

	rand.Seed(now.Time())

	runSetup()
	router := setupRouter()
	err = router.Run(config.GinHostPort)
	if err != nil {
		log.Fatal("Troubles with Gin Server")
	}
}

func checkErrCron(err error, schedulerName string, timing uint64) {
	if err != nil {
		log.Fatal("Troubles with setting scheduler", schedulerName)
	} else {
		log.Println("[DEBUG] schedule planned for", schedulerName, "function with period of", timing, "seconds.")
	}
}

func executeCronJob() {
	s := gocron.NewScheduler(time.UTC)
	_, err := s.Every(config.LoadProxiesTime).Seconds().Do(reloadProxies)
	checkErrCron(err, "reloadProxies", config.LoadProxiesTime)
	_, err = s.Every(config.LogStatsTime).Seconds().Do(stats.LogStats)
	checkErrCron(err, "logStats", config.LogStatsTime)
	_, err = s.Every(config.ReturnPostponedTime).Seconds().Do(returnPostponedWithCondition)
	checkErrCron(err, "returnPostponedWithCondition", config.ReturnPostponedTime)
	_, err = s.Every(config.SaveToMongoTime).Seconds().Do(db.Save)
	checkErrCron(err, "saveToMongo", config.SaveToMongoTime)
	s.StartAsync()
}

func setupRouter() (r *gin.Engine) {
	r = gin.Default()
	r.Use(stats.RequestStats())
	r.GET("/", indexPage)
	r.GET("/get-random", routeGetRandom)
	r.GET("/inc-good-attempts", routeIncGoodAttempts)
	r.GET("/mark-dead", routeMarkDead)
	r.GET("/reanimate", routeReanimate)
	r.GET("/start", start)
	r.GET("/alive-from-dead", routeAliveFromDead)
	r.GET("/remove-days", removeDays)
	r.GET("/max-good-attempts", changeMaxGoodAttempts)
	r.GET("/remove-dead", removeDead)
	r.GET("/reload-proxy-list", reloadProxyList)
	r.GET("/get-working-list", getWorkingList)
	r.GET("/get-dead-list", getDeadList)
	r.GET("/get-proxy-usefulness-stats", getProxyUsefulnessStats)
	r.GET("/clear-usefulness-stats", routeClearUsefulnessStats)
	r.GET("/zstats", showStatsForZabbix)
	r.GET("/hstats", showHTMLStats)
	r.GET("/stats", metrics)
	r.POST("/add-proxies", routeAddProxies)
	r.POST("/remove-proxies", routeRemoveProxies)
	return
}

func metrics(c *gin.Context) {
	c.JSON(http.StatusOK, stats.Report())
}

func indexPage(c *gin.Context) {
	c.String(http.StatusOK, "PMServer - GO version - Hello")
}

func routeGetRandom(c *gin.Context) {
	scraper := c.Query("scraper")
	if scraper == "" {
		c.String(http.StatusForbidden, "Field scraper is empty")
		return
	}
	randomProxy, err := manager.GetRandomProxy(scraper)
	if err != nil {
		c.String(http.StatusNoContent, "")
	} else {
		c.String(http.StatusOK, randomProxy)
	}
}

func routeIncGoodAttempts(c *gin.Context) {
	scraper := c.Query("scraper")
	proxy := c.Query("proxy")
	if (scraper == "") || (proxy == "") {
		c.String(http.StatusForbidden, "Field scraper or proxy is empty")
		return
	}
	manager.IncGoodAttempts(scraper, proxy)
	c.String(http.StatusOK, "OK")
}

func routeMarkDead(c *gin.Context) {
	scraper := c.Query("scraper")
	proxy := c.Query("proxy")
	if (scraper == "") || (proxy == "") {
		c.String(http.StatusForbidden, "Field scraper or proxy is empty")
		return
	}
	manager.MarkDead(scraper, proxy)
	c.String(http.StatusOK, "OK")
}

func routeReanimate(c *gin.Context) {
	scraper := c.Query("scraper")
	if scraper == "" {
		c.String(http.StatusForbidden, "Field scraper is empty")
		return
	}
	manager.ReanimateProxies(scraper)
	c.String(http.StatusOK, "OK")
}

func routeAliveFromDead(c *gin.Context) {
	scraper := c.Query("scraper")
	if scraper == "" {
		c.String(http.StatusForbidden, "Field scraper is empty")
		return
	}
	db.Base.AliveFromDead(scraper)
	c.String(http.StatusOK, "OK")
}

func removeDays(c *gin.Context) {
	days := c.Query("days")
	if days == "" {
		c.String(http.StatusForbidden, "Field days is empty")
		return
	}
	daysInt64, err := strconv.ParseInt(days, 10, 64)
	if err == nil {
		log.Printf("[DEBUG] New clearance time set to %d days.\n", daysInt64)
		config.RemoveDeadTime = daysInt64 * 24 * 60 * 60
	}
	c.String(http.StatusOK, "OK")
}

func changeMaxGoodAttempts(c *gin.Context) {
	numbers := c.Query("numbers")
	if numbers == "" {
		c.String(http.StatusForbidden, "Field numbers is empty")
		return
	}
	maxAttempts, err := strconv.ParseInt(numbers, 10, 32)
	if err == nil {
		log.Printf("[DEBUG] New max attempts value set to %d.\n", maxAttempts)
		config.MaxGoodAttempts = int32(maxAttempts)
	}
	c.String(http.StatusOK, "OK")
}

func removeDead(c *gin.Context) {
	scraper := c.Query("scraper")
	if scraper == "" {
		c.String(http.StatusForbidden, "Field scraper is empty")
		return
	}
	manager.RemoveDeadProxiesForALongTime(scraper)
	c.String(http.StatusOK, "OK")
}

func reloadProxyList(c *gin.Context) {
	reloadProxies()
	c.String(http.StatusOK, "OK")
}

func start(c *gin.Context) {
	scraper := c.Query("scraper")
	log.Printf("[DEBUG] %s\n", scraper)
}

func getWorkingList(c *gin.Context) {
	scraper := c.Query("scraper")
	if scraper == "" {
		c.String(http.StatusForbidden, "Field scraper is empty")
		return
	}
	workingList := db.Set.GetWorking(scraper)
	sort.Strings(workingList)
	outputLines := strings.Join(workingList, "\n")
	c.String(http.StatusOK, outputLines)
}

func getDeadList(c *gin.Context) {
	scraper := c.Query("scraper")
	if scraper == "" {
		c.String(http.StatusForbidden, "Field scraper is empty")
		return
	}
	deadList := db.Set.GetDead(scraper)
	sort.Strings(deadList)
	outputLines := strings.Join(deadList, "\n")
	c.String(http.StatusOK, outputLines)
}

func getProxyUsefulnessStats(c *gin.Context) {
	var name string
	var statsOrders = []string{"name", "sdate", "success", "fdate", "fail"}
	scraper := c.Query("scraper")
	orderBy := c.DefaultQuery("order_by", "name")
	found := find(statsOrders, orderBy)
	if scraper == "" || !found {
		c.String(http.StatusForbidden, "Field scraper is empty or wrong orderBy")
		return
	}
	stats.ProxyUsefulnessStatsToCSV(scraper, orderBy)
	saveTime := stats.UnixTimeString(now.Time())
	name = scraper + "_" + saveTime + "_" + config.StatsFileName
	c.FileAttachment(config.StatsFileName, name)
	c.String(http.StatusOK, "OK")
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
func routeAddProxies(c *gin.Context) {
	var json struct {
		Proxies []string `json:"proxies,omitempty"`
		Scraper string   `json:"scraper,omitempty"`
	}
	err := c.BindJSON(&json)
	if err != nil {
		c.String(http.StatusForbidden, "Something went wrong")
		return
	}
	if len(json.Proxies) == 0 {
		c.String(http.StatusForbidden, "Empty proxies list")
		return
	}
	db.StoreProxies(json.Scraper, json.Proxies)
	c.String(http.StatusOK, "OK")
}

// routeRemoveProxies for adding list of proxies to scraper
// format {"scraper": <name>, "proxies": [list_of_proxies]}
// if the field "scraper" is not specified, the proxy list applies to all scrapers.
func routeRemoveProxies(c *gin.Context) {
	var json struct {
		Proxies []string `json:"proxies,omitempty"`
		Scraper string   `json:"scraper,omitempty"`
	}
	err := c.BindJSON(&json)
	if err != nil {
		c.String(http.StatusForbidden, "Something went wrong")
		return
	}
	if len(json.Proxies) == 0 {
		c.String(http.StatusForbidden, "Empty proxies list")
		return
	}
	db.Base.RemoveProxies(json.Scraper, json.Proxies)
	c.String(http.StatusOK, "OK")
}

func reloadProxies() {
	var proxies []string
	var scraperToAdd = ""
	log.Println("[DEBUG] Reloading Proxies")
	proxies = loadProxiesFromWeb(config.ResourceLink)
	db.StoreProxies(scraperToAdd, proxies)
}

func loadProxiesFromWeb(resourceLink string) (proxies []string) {
	req, err := http.NewRequest("GET", resourceLink, http.NoBody)
	if err != nil {
		return
	}
	req.SetBasicAuth(config.ProxyListUsername, config.ProxyListPassword)
	cli := &http.Client{}
	resp, err := cli.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}
	proxies = strings.Split(string(body), "\n")
	return
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
		log.Printf("[INFO] [%s] percent of available proxies from busy and postponed %.2f%%\n", scraper, percentOfAvailableProxiesRest*percent)
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
