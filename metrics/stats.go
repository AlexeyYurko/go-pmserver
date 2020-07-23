package stats

import (
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/AlexeyYurko/go-pmserver/config"
	"github.com/AlexeyYurko/go-pmserver/db"
	"github.com/AlexeyYurko/go-pmserver/now"
	"github.com/jedib0t/go-pretty/table"

	"github.com/gin-gonic/gin"
	"github.com/rcrowley/go-metrics"
)

const (
	ginLatencyMetric = "gin.latency"
	ginStatusMetric  = "gin.status"
	ginRequestMetric = "gin.request"
)
const (
	unchecked = "unchecked"
	dead      = "dead"
	busy      = "busy"
	postponed = "postponed"
	good      = "good"
	available = "available"
)

// Report from default metric registry
func Report() metrics.Registry {
	return metrics.DefaultRegistry
}

// RequestStats middleware
func RequestStats() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		c.Next()

		req := metrics.GetOrRegisterMeter(ginRequestMetric, nil)
		req.Mark(1)

		latency := metrics.GetOrRegisterTimer(ginLatencyMetric, nil)
		latency.UpdateSince(start)

		status := metrics.GetOrRegisterMeter(fmt.Sprintf("%s.%d", ginStatusMetric, c.Writer.Status()), nil)
		status.Mark(1)
	}
}

type statRecord struct {
	Proxy                  string
	LastSuccessfullyUsed   string
	NumberOfSuccessfulUses string
	LastFailureUsed        string
	NumberOfFailures       string
}
type outputForStat map[string]map[string]int

var statsFilename = "success_stats.csv"

func makeProxiesNumbersData(scraper string) outputForStat {
	var outputs = make(map[string]map[string]int)
	var mainStatuses = []string{available, good, postponed, busy, dead, unchecked}
	var proxyTypes = []string{"all", "rack", "free"}

	for _, proxyType := range proxyTypes {
		outputs[proxyType] = make(map[string]int)
		for _, status := range mainStatuses {
			outputs[proxyType][status] = db.Set.LengthWithProxyRackAffected(scraper, status, proxyType)
		}
	}
	outputs["all"]["total"] = outputs["all"]["available"] + outputs["all"]["busy"] + outputs["all"]["postponed"] + outputs["all"]["dead"]
	outputs["rack"]["total"] = outputs["rack"]["available"] + outputs["rack"]["busy"] + outputs["rack"]["postponed"] + outputs["rack"]["dead"]
	outputs["free"]["total"] = outputs["free"]["available"] + outputs["free"]["busy"] + outputs["free"]["postponed"] + outputs["free"]["dead"]
	return outputs
}

// LogStats output stats to console
func LogStats() {
	toLogStatuses := []string{good, unchecked, available, busy, postponed, dead, "total"}
	var proxyTypes = []string{"all", "rack", "free"}

	for _, scraper := range config.Scrapers {
		lines := makeProxiesNumbersData(scraper)
		total := lines["all"]["total"]
		if total == 0 {
			log.Printf("[INFO] [%s] no data at all.", scraper)
			continue
		}
		for _, proxyType := range proxyTypes {
			for _, status := range toLogStatuses {
				proxies := lines[proxyType][status]
				proxiesPercent := float64(proxies) / float64(total) * 100
				log.Printf("[INFO] [%s]-[%s]-[%s]-proxies %d, %.2f", scraper, proxyType, status, proxies, proxiesPercent)
			}
		}
	}
}

// HTMLStats outputs stats to html response
func HTMLStats() (output string) {
	toLogStatuses := []string{good, unchecked, available, busy, postponed, dead, "total"}
	for _, scraper := range config.Scrapers {
		output += fmt.Sprintf("<br><br><strong>%s</strong><br>", scraper)
		t := table.NewWriter()
		t.AppendHeader(table.Row{"Status", "Numbers all", "%", "Numbers free", "% free", "Numbers rack", "% rack"})
		t.SetStyle(table.StyleLight)
		lines := makeProxiesNumbersData(scraper)
		total := lines["all"]["total"]
		if total == 0 {
			log.Printf("[%s] no data at all.\n", scraper)
			continue
		}
		for _, status := range toLogStatuses {
			name := status
			percent := 100.0
			allProxies := lines["all"][status]
			allProxiesPercent := math.Round(float64(allProxies) / float64(total) * percent)
			freeProxies := lines["free"][status]
			freeProxiesPercent := math.Round(float64(freeProxies) / float64(total) * percent)
			rackProxies := lines["rack"][status]
			rackProxiesPercent := math.Round(float64(rackProxies) / float64(total) * percent)
			t.AppendRow([]interface{}{name, allProxies, allProxiesPercent, freeProxies, freeProxiesPercent, rackProxies, rackProxiesPercent})
		}
		output += t.RenderHTML()
	}
	return output
}

// ProxyUsefulnessStatsToCSV output internal stats to CSV
func ProxyUsefulnessStatsToCSV(scraper, orderBy string) {
	var stats []statRecord
	for proxy, record := range db.Base.RangeScraper(scraper) {
		lineRecord := statRecord{
			Proxy:                  proxy,
			LastSuccessfullyUsed:   UnixTimeString(record.LastSuccessfullyUsed),
			NumberOfSuccessfulUses: strconv.Itoa(int(record.NumberOfSuccessfulUses)),
			LastFailureUsed:        UnixTimeString(record.LastFailureUsed),
			NumberOfFailures:       strconv.Itoa(int(record.NumberOfFailures)),
		}
		stats = append(stats, lineRecord)
	}
	if orderBy == "name" {
		sort.SliceStable(stats, func(i, j int) bool {
			return stats[i].Proxy < stats[j].Proxy
		})
	} else {
		sort.SliceStable(stats, func(i, j int) bool {
			return stats[i].Proxy < stats[j].Proxy
		})
		switch orderBy {
		case "sdate":
			sort.SliceStable(stats, func(i, j int) bool {
				return stats[i].LastSuccessfullyUsed > stats[j].LastSuccessfullyUsed
			})
		case "success":
			sort.SliceStable(stats, func(i, j int) bool {
				return stats[i].NumberOfSuccessfulUses > stats[j].NumberOfSuccessfulUses
			})
		case "fdate":
			sort.SliceStable(stats, func(i, j int) bool {
				return stats[i].LastFailureUsed > stats[j].LastFailureUsed
			})
		case "fail":
			sort.SliceStable(stats, func(i, j int) bool {
				return stats[i].NumberOfFailures > stats[j].NumberOfFailures
			})
		}
	}
	writeCSV(stats)
}

func writeCSV(data []statRecord) {
	file, err := os.Create(statsFilename)
	if err != nil {
		log.Fatal("Cannot create file", err)
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	var header = []string{"proxy", "last_successfully_used", "number_of_successful_uses", "last_failure_used", "number_of_failures"}
	_ = writer.Write(header)
	for _, record := range data {
		line := []string{
			record.Proxy,
			record.LastSuccessfullyUsed,
			record.NumberOfSuccessfulUses,
			record.LastFailureUsed,
			record.NumberOfFailures,
		}
		_ = writer.Write(line)
	}
}

// GetStatsForTimeWithinBusyPostponed internal stats
func GetStatsForTimeWithinBusyPostponed(scraper string) map[string]int64 {
	var timeouts []int64
	currentTime := now.Time()
	busyPostponed := db.Set.LoadBusyPostponed(scraper)
	for _, proxy := range busyPostponed {
		nextCheck := db.Base.LoadNextCheck(scraper, proxy)
		timeouts = append(timeouts, nextCheck-currentTime)
	}
	return getStats(timeouts)
}

func getStats(timesList []int64) (statsTiming map[string]int64) {
	if len(timesList) == 0 {
		statsTiming = map[string]int64{"max": 0, "min": 0, "mean": 0, "median": 0, "stdev": 0}
		return
	}
	min, max := countMinMax(timesList)
	mean := countMean(timesList)
	median := countMedian(timesList)
	stdev := countStDev(timesList, mean)
	statsTiming = map[string]int64{"max": max, "min": min, "mean": mean, "median": median, "stdev": stdev}
	return statsTiming
}

// UnixTimeString outputs time in unix format
func UnixTimeString(unixTime int64) string {
	if unixTime == 0 {
		return "never"
	}
	unixTimeUTC := time.Unix(unixTime, 0)
	unitTimeInRFC3339 := unixTimeUTC.Format(time.RFC3339)
	return unitTimeInRFC3339
}

func countMinMax(array []int64) (min, max int64) {
	max = array[0]
	min = array[0]
	for _, value := range array {
		if max < value {
			max = value
		}
		if min > value {
			min = value
		}
	}
	return min, max
}

func countMean(array []int64) (mean int64) {
	var sum int64
	for _, num := range array {
		sum += num
	}
	mean = sum / int64(len(array))
	return
}

func countMedian(array []int64) (median int64) {
	sort.Slice(array, func(i, j int) bool { return array[i] < array[j] })
	l := len(array)
	switch {
	case l == 0:
		median = 0
	case l%2 == 0:
		median = countMean(array[(l/2)-1 : (l/2)+1])
	case l%2 != 0:
		median = array[l/2]
	}
	return median
}

func countStDev(array []int64, mean int64) (stdev int64) {
	if (len(array) > 2) && (mean != 0) {
		var sum float64
		for _, value := range array {
			sum += math.Pow(float64(value-mean), 2.0)
		}
		stdev = int64(math.Pow(sum/float64(len(array)), 0.5))
	}
	return
}
