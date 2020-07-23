package db

import (
	"errors"
	"math/rand"
	"strings"
	"sync"
)

type statusSet struct {
	*sync.RWMutex
	set map[string]map[string]map[string]bool
}

// Set map with proxy statuses
var Set statusSet

func (c *statusSet) Load(scraper, status, proxy string) (value bool) {
	c.RLock()
	defer c.RUnlock()
	value = c.set[scraper][status][proxy]
	return value
}

func (c *statusSet) Store(scraper, status, proxy string) {
	c.Lock()
	defer c.Unlock()
	c.set[scraper][status][proxy] = true
}

func (c *statusSet) Delete(scraper, status, proxy string) {
	c.Lock()
	defer c.Unlock()
	delete(c.set[scraper][status], proxy)
}

func (c *statusSet) Length(scraper, status string) (length int) {
	c.RLock()
	defer c.RUnlock()
	length = len(c.set[scraper][status])
	return
}

func (c *statusSet) Range(scraper, status string) (proxyList []string) {
	c.RLock()
	defer c.RUnlock()
	proxyStatusRange := c.set[scraper][status]
	for proxy := range proxyStatusRange {
		proxyList = append(proxyList, proxy)
	}
	return
}

func (c *statusSet) LengthWithProxyRackAffected(scraper, status, proxyType string) int {
	c.RLock()
	copySet := make(map[string]map[string]map[string]bool)
	for k, v := range c.set {
		copySet[k] = v
	}
	c.RUnlock()
	switch {
	case status == "" && proxyType == proxyTypeAll:
		return len(copySet[scraper])
	case status == "" && proxyType == proxyTypeFree:
		return len(copySet[scraper]) - c.proxyRackLength(scraper)
	case status == "" && proxyType == proxyTypeRack:
		return c.proxyRackLength(scraper)
	case proxyType == proxyTypeAll:
		return len(copySet[scraper][status])
	case proxyType == proxyTypeFree:
		return len(copySet[scraper][status]) - c.proxyRackLengthWithStatus(scraper, status)
	case proxyType == proxyTypeRack:
		return c.proxyRackLengthWithStatus(scraper, status)
	}
	return 0
}

func (c *statusSet) proxyRackLength(scraper string) (counter int) {
	for proxy := range c.set[scraper] {
		if c.Load(scraper, isProxyrack, proxy) {
			counter++
		}
	}
	return
}

func (c *statusSet) proxyRackLengthWithStatus(scraper, status string) (counter int) {
	for _, proxy := range c.Range(scraper, status) {
		if c.Load(scraper, isProxyrack, proxy) {
			counter++
		}
	}
	return
}

func (c *statusSet) ProxyInPostponed(scraper, proxy string) bool {
	return c.Load(scraper, postponed, proxy)
}

func (c *statusSet) ProxyAlreadyGood(scraper, proxy string) bool {
	return c.Load(scraper, good, proxy)
}

func (c *statusSet) ProxyAlreadyDead(scraper, proxy string) bool {
	return c.Load(scraper, dead, proxy)
}

func (c *statusSet) status(scraper, proxy, toStatus string) {
	var mainStatuses = []string{available, good, postponed, busy, dead, unchecked}
	for _, status := range mainStatuses {
		c.Delete(scraper, status, proxy)
	}
	c.Store(scraper, toStatus, proxy)
	var statusToAddToAvailable = []string{unchecked, good}
	if found := find(statusToAddToAvailable, toStatus); found {
		c.Store(scraper, available, proxy)
	}
}

func (c *statusSet) GetRandomKey(scraper string) (string, error) {
	length := c.Length(scraper, available)
	if length == 0 {
		return "", errors.New("no proxies")
	}
	randomPosition := rand.Intn(length)
	counter := 0
	c.RLock()
	defer c.RUnlock()
	for key := range c.set[scraper][available] {
		if counter == randomPosition {
			return key, nil
		}
		counter++
	}
	return "", errors.New("no proxies")
}

func (c *statusSet) ProxiesExceptDeadSize(scraper string) int {
	return c.busyPostponedSize(scraper) + c.AvailableSize(scraper)
}

func (c *statusSet) busyPostponedSize(scraper string) int {
	return c.Length(scraper, busy) + c.Length(scraper, postponed)
}

func (c *statusSet) AvailableSize(scraper string) int {
	return c.Length(scraper, available)
}

func (c *statusSet) GetDead(scraper string) (deadProxies []string) {
	deadProxies = c.Range(scraper, dead)
	return
}

func (c *statusSet) GetWorking(scraper string) (workingProxies []string) {
	workingProxies = append(workingProxies, c.Range(scraper, busy)...)
	workingProxies = append(workingProxies, c.Range(scraper, postponed)...)
	workingProxies = append(workingProxies, c.Range(scraper, available)...)
	return
}

func (c *statusSet) LoadBusyPostponed(scraper string) (busyAndPostponedProxies []string) {
	busyAndPostponedProxies = append(busyAndPostponedProxies, c.Range(scraper, busy)...)
	busyAndPostponedProxies = append(busyAndPostponedProxies, c.Range(scraper, postponed)...)
	return
}

func (c *statusSet) BusyAndPostponedSize(scraper string) (busyAndPostponedLength int) {
	busyAndPostponedLength = c.Length(scraper, busy) + c.Length(scraper, postponed)
	return
}

func (c *statusSet) Good(scraper, proxy string) {
	c.status(scraper, proxy, good)
}

func (c *statusSet) Dead(scraper, proxy string) {
	c.status(scraper, proxy, dead)
}

func (c *statusSet) Unchecked(scraper, proxy string) {
	c.status(scraper, proxy, unchecked)
}

func (c *statusSet) Busy(scraper, proxy string) {
	c.status(scraper, proxy, busy)
}

func (c *statusSet) Postponed(scraper, proxy string) {
	c.status(scraper, proxy, postponed)
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
