package db

import (
	"log"
	"sync"
	"sync/atomic"

	"github.com/AlexeyYurko/go-pmserver/config"
	"github.com/AlexeyYurko/go-pmserver/now"
)

const (
	unchecked     = "unchecked"
	dead          = "dead"
	busy          = "busy"
	postponed     = "postponed"
	good          = "good"
	available     = "available"
	isProxyrack   = "is_proxyrack"
	proxyTypeAll  = "all"
	proxyTypeRack = "rack"
	proxyTypeFree = "free"
)

var (
	// GoodPostponeTimeoutsForStats internal stats
	GoodPostponeTimeoutsForStats map[string][]int64
	// ProxySuccessUsageTimeForStats internal stats
	ProxySuccessUsageTimeForStats map[string][]int64
	// TimeStatsForUnavailableProxies internal stats
	TimeStatsForUnavailableProxies map[string][]int64
	// SuccessfulGetRandomProxyRequestRate internal stats
	SuccessfulGetRandomProxyRequestRate map[string]RequestsRate
)

// RequestsRate for internal request stats
type RequestsRate struct {
	TimeForStartCounting int64
	Counter              int
}

// proxy for store info only about proxy inside scraper
type proxy struct {
	Status                 string
	StartGetProxyTime      int64
	NextCheck              int64
	GoodAttempts           int32
	FailedAttempts         int32
	LastSuccessfullyUsed   int64
	NumberOfSuccessfulUses int32
	LastFailureUsed        int64
	NumberOfFailures       int32
}

type localBase struct {
	*sync.RWMutex
	base map[string]map[string]proxy
}

// Base main internal memory structure
var Base localBase

func (c *localBase) Store(scraper, proxy string, value proxy) {
	c.Lock()
	defer c.Unlock()
	c.base[scraper][proxy] = value
}

func (c *localBase) Delete(scraper, proxy string) {
	c.Lock()
	defer c.Unlock()
	delete(c.base[scraper], proxy)
}

func (c *localBase) Exist(scraper, proxy string) (proxyExist bool) {
	c.RLock()
	defer c.RUnlock()
	_, proxyExist = c.base[scraper][proxy]
	return
}

func (c *localBase) ProxyTime(scraper, proxy string) (proxyTime int64) {
	c.RLock()
	defer c.RUnlock()
	proxyTime = c.base[scraper][proxy].StartGetProxyTime
	return
}

func (c *localBase) ProxyNotInBase(scraper, proxy string) bool {
	c.RLock()
	defer c.RUnlock()
	if _, ok := c.base[scraper][proxy]; !ok {
		log.Printf("[DEBUG] [%s]: proxy %s was not found in proxy list.\n", scraper, proxy)
		return true
	}
	return false
}

func (c *localBase) rangeProxyInScraper(scraper string) (proxiesInScraper []string) {
	c.RLock()
	defer c.RUnlock()
	for proxy := range c.base[scraper] {
		proxiesInScraper = append(proxiesInScraper, proxy)
	}
	return
}

func (c *localBase) RangeScraper(scraper string) (proxiesInScraper map[string]proxy) {
	c.RLock()
	defer c.RUnlock()
	proxiesInScraper = make(map[string]proxy)
	for proxy, record := range c.base[scraper] {
		proxiesInScraper[proxy] = record
	}
	return
}

func (c *localBase) ProxyTimeToNow(scraper, proxy string) {
	c.Lock()
	defer c.Unlock()
	pInfo := c.base[scraper][proxy]
	atomic.StoreInt64(&pInfo.StartGetProxyTime, now.Time())
	c.base[scraper][proxy] = pInfo
}

func (c *localBase) IncProxyGoodAttempts(scraper, proxy string) (attempts int32) {
	c.Lock()
	defer c.Unlock()
	pInfo := c.base[scraper][proxy]
	atomic.AddInt32(&pInfo.GoodAttempts, 1)
	atomic.AddInt32(&pInfo.NumberOfSuccessfulUses, 1)
	atomic.StoreInt64(&pInfo.LastSuccessfullyUsed, now.Time())
	c.base[scraper][proxy] = pInfo
	attempts = pInfo.GoodAttempts
	return
}

func (c *localBase) IncFailureAttempts(scraper, proxy string) {
	c.Lock()
	defer c.Unlock()
	pInfo := c.base[scraper][proxy]
	atomic.AddInt32(&pInfo.FailedAttempts, 1)
	atomic.AddInt32(&pInfo.NumberOfFailures, 1)
	atomic.StoreInt64(&pInfo.LastFailureUsed, now.Time())
	c.base[scraper][proxy] = pInfo
}

func (c *localBase) FailedAttempts(scraper, proxy string) (failedAttempts int32) {
	c.Lock()
	defer c.Unlock()
	failedAttempts = c.base[scraper][proxy].FailedAttempts
	return
}

func (c *localBase) LoadNextCheck(scraper, proxy string) (nextCheck int64) {
	c.RLock()
	defer c.RUnlock()
	nextCheck = c.base[scraper][proxy].NextCheck
	return
}

func (c *localBase) StoreNextCheck(scraper, proxy string, nextCheck int64) {
	c.Lock()
	defer c.Unlock()
	pInfo := c.base[scraper][proxy]
	atomic.StoreInt64(&pInfo.NextCheck, nextCheck)
	c.base[scraper][proxy] = pInfo
}

func (c *localBase) CleanProxyInfo(scraper, proxy string) {
	c.Lock()
	defer c.Unlock()
	pInfo := c.base[scraper][proxy]
	atomic.StoreInt32(&pInfo.GoodAttempts, 0)
	atomic.StoreInt64(&pInfo.NextCheck, 0)
	atomic.StoreInt64(&pInfo.StartGetProxyTime, 0)
	c.base[scraper][proxy] = pInfo
	Set.Unchecked(scraper, proxy)
}

func (c *localBase) CleanGoodAttempts(scraper, proxy string) {
	c.Lock()
	defer c.Unlock()
	pInfo := c.base[scraper][proxy]
	atomic.StoreInt32(&pInfo.GoodAttempts, 0)
	c.base[scraper][proxy] = pInfo
}

func (c *localBase) CleanNextCheck(scraper, proxy string) {
	c.Lock()
	defer c.Unlock()
	pInfo := c.base[scraper][proxy]
	atomic.StoreInt64(&pInfo.NextCheck, 0)
	c.base[scraper][proxy] = pInfo
}

func (c *localBase) RemoveProxies(scraperToRemove string, proxyList []string) {
	var scrapersToRemove []string
	if scraperToRemove == "" {
		scrapersToRemove = append(scrapersToRemove, config.Scrapers...)
	} else {
		scrapersToRemove = append(scrapersToRemove, scraperToRemove)
	}

	for _, scraper := range scrapersToRemove {
		for _, proxy := range proxyList {
			c.removeProxy(scraper, proxy)
		}
		Remove(scraper, proxyList)
	}
}

func (c *localBase) removeProxy(scraper, proxy string) {
	c.Delete(scraper, proxy)
	var statuses = []string{available, good, postponed, busy, dead, unchecked, isProxyrack}
	for _, status := range statuses {
		Set.Delete(scraper, status, proxy)
	}
}

func (c *localBase) AliveFromDead(scraper string) {
	log.Printf("[DEBUG] [%s] Move all proxies from 'dead' to 'unchecked'.\n", scraper)
	for _, proxy := range Set.GetDead(scraper) {
		c.Lock()
		pInfo := c.base[scraper][proxy]
		atomic.StoreInt64(&pInfo.StartGetProxyTime, 0)
		atomic.StoreInt64(&pInfo.NextCheck, 0)
		atomic.StoreInt32(&pInfo.GoodAttempts, 0)
		atomic.StoreInt32(&pInfo.FailedAttempts, 0)
		c.base[scraper][proxy] = pInfo
		c.Unlock()
		Set.Unchecked(scraper, proxy)
	}
}

func (c *localBase) ClearUsefulnessStats() {
	for _, scraper := range config.Scrapers {
		for _, proxy := range c.rangeProxyInScraper(scraper) {
			c.Lock()
			pInfo := c.base[scraper][proxy]
			atomic.StoreInt64(&pInfo.LastSuccessfullyUsed, 0)
			atomic.StoreInt32(&pInfo.NumberOfSuccessfulUses, 0)
			atomic.StoreInt64(&pInfo.LastFailureUsed, 0)
			atomic.StoreInt32(&pInfo.NumberOfFailures, 0)
			c.base[scraper][proxy] = pInfo
			c.Unlock()
		}
	}
}

// Init fill initial maps and arrays
func Init() {
	Base = localBase{
		&sync.RWMutex{},
		make(map[string]map[string]proxy)}
	Set = statusSet{
		&sync.RWMutex{},
		make(map[string]map[string]map[string]bool)}
	Set.set = make(map[string]map[string]map[string]bool)
	GoodPostponeTimeoutsForStats = make(map[string][]int64)
	ProxySuccessUsageTimeForStats = make(map[string][]int64)
	TimeStatsForUnavailableProxies = make(map[string][]int64)
	SuccessfulGetRandomProxyRequestRate = make(map[string]RequestsRate)
	successfulGetStat := RequestsRate{
		TimeForStartCounting: 0,
		Counter:              0,
	}
	var statuses = []string{available, good, postponed, busy, dead, unchecked, isProxyrack}
	for _, scraper := range config.Scrapers {
		Base.base[scraper] = make(map[string]proxy)
		Set.set[scraper] = make(map[string]map[string]bool)
		for _, status := range statuses {
			Set.set[scraper][status] = make(map[string]bool)
		}
		SuccessfulGetRandomProxyRequestRate[scraper] = successfulGetStat
	}
}

// StoreProxies put new proxies to local db
func StoreProxies(scraperToAdd string, proxyList []string) {
	var scrapersToAdd []string

	if scraperToAdd == "" {
		scrapersToAdd = append(scrapersToAdd, config.Scrapers...)
	} else {
		scrapersToAdd = append(scrapersToAdd, scraperToAdd)
	}

	log.Printf("[DEBUG] Loaded %d records.\n", len(proxyList))
	proxyInfo := proxy{
		Status:                 unchecked,
		StartGetProxyTime:      0,
		NextCheck:              0,
		GoodAttempts:           0,
		FailedAttempts:         0,
		LastSuccessfullyUsed:   0,
		NumberOfSuccessfulUses: 0,
		LastFailureUsed:        0,
		NumberOfFailures:       0,
	}
	for _, scraper := range scrapersToAdd {
		counter := 0
		for _, currentProxy := range proxyList {
			if found := Base.Exist(scraper, currentProxy); found {
				continue
			}
			if InProxyrack(currentProxy) {
				if config.UseProxyRack {
					Set.Store(scraper, isProxyrack, currentProxy)
				} else {
					continue
				}
			}
			Base.Store(scraper, currentProxy, proxyInfo)
			Set.Unchecked(scraper, currentProxy)
			counter++
		}
		log.Printf("[DEBUG] To [%s] added new %d records.\n", scraper, counter)
	}
}

// InProxyrack checks if the proxy belongs to the proxyrack service
func InProxyrack(proxy string) bool {
	found := find(config.ProxyrackProxyIP, proxy)
	return found
}
