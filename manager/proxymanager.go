package manager

import (
	"math"
	"math/rand"

	"github.com/rs/zerolog/log"

	"github.com/AlexeyYurko/go-pmserver/config"
	"github.com/AlexeyYurko/go-pmserver/db"
	"github.com/AlexeyYurko/go-pmserver/now"
)

var busyPostponeTimeoutCapSec float32 = 10.0

// GetRandomProxy finds random proxy in available list
// TODO refactor available list to speed ups
func GetRandomProxy(scraper string) (randomProxy string, err error) {
	if randomProxy, err = db.Set.GetRandomKey(scraper); err != nil {
		db.TimeStatsForUnavailableProxies[scraper] = append(db.TimeStatsForUnavailableProxies[scraper], now.Time())
		log.Info().Str("scraper", scraper).Msg("there is no good/unchecked proxy available")
	} else {
		var timeForStartCounting int64
		var newCounter int

		if counter := db.SuccessfulGetRandomProxyRequestRate[scraper].Counter; counter != 0 {
			timeForStartCounting = db.SuccessfulGetRandomProxyRequestRate[scraper].TimeForStartCounting
			newCounter = counter + 1
		} else {
			timeForStartCounting = now.Time()
			newCounter = 1
		}
		successfulGetStat := db.RequestsRate{
			TimeForStartCounting: timeForStartCounting,
			Counter:              newCounter,
		}
		db.SuccessfulGetRandomProxyRequestRate[scraper] = successfulGetStat

		db.Set.Busy(scraper, randomProxy)
		postponeReturnFromBusyToGood(scraper, randomProxy, true)
	}
	return
}

func postponeReturnFromBusyToGood(scraper, proxy string, initial bool) {
	currentTime := now.Time()
	postponeTime := now.Time()
	proxyTime := db.Base.ProxyTime(scraper, proxy)
	if db.Set.ProxiesExceptDeadSize(scraper) > 0 {
		postponeTimeout := randomUniform(busyPostponeTimeoutCapSec)
		postponeTime = proxyTime + postponeTimeout
	}
	if currentTime+1 > postponeTime {
		postponeTime = currentTime + 1
	}
	db.GoodPostponeTimeoutsForStats[scraper] = append(db.GoodPostponeTimeoutsForStats[scraper], postponeTime-proxyTime)
	db.Base.StoreNextCheck(scraper, proxy, postponeTime)
	if !initial {
		db.Base.ProxyTimeToNow(scraper, proxy)
	}
}

// IncGoodAttempts increase good proxy statistics and counter
func IncGoodAttempts(scraper, proxy string) {
	if db.Base.ProxyNotInBase(scraper, proxy) {
		return
	}

	localProxyGoodAttempts := db.Base.IncProxyGoodAttempts(scraper, proxy)
	db.ProxySuccessUsageTimeForStats[scraper] = append(db.ProxySuccessUsageTimeForStats[scraper], now.Time()-db.Base.ProxyTime(scraper, proxy))
	if localProxyGoodAttempts >= config.MaxGoodAttempts {
		log.Debug().
			Str("scraper", scraper).
			Int32("attempts", localProxyGoodAttempts).
			Str("proxy", proxy).
			Msg("good attempts. GOOD proxy moved to POSTPONED")
		markPostponed(scraper, proxy)
	} else {
		log.Debug().
			Str("scraper", scraper).
			Int32("attempts", localProxyGoodAttempts).
			Str("proxy", proxy).
			Msg("good attempts")
		markGood(scraper, proxy)
	}
}

func markPostponed(scraper, proxy string) {
	log.Debug().
		Str("scraper", scraper).
		Str("proxy", proxy).
		Msg("GOOD proxy became POSTPONED")
	nextCheck := now.Time() + config.BackoffTimeForGoodAttempts
	db.Base.StoreNextCheck(scraper, proxy, nextCheck)
	db.Set.Postponed(scraper, proxy)
}

func markGood(scraper, proxy string) {
	if db.Set.ProxyAlreadyGood(scraper, proxy) {
		log.Debug().
			Str("scraper", scraper).
			Str("proxy", proxy).
			Msg("proxy is always GOOD")
		return
	}
	db.Set.Good(scraper, proxy)
	postponeReturnFromBusyToGood(scraper, proxy, false)
	log.Debug().
		Str("scraper", scraper).
		Str("proxy", proxy).
		Msg("proxy set to GOOD")
}

// MarkDead increase bad proxy statistics and counter
func MarkDead(scraper, proxy string) {
	if db.Base.ProxyNotInBase(scraper, proxy) {
		return
	}

	db.Base.IncFailureAttempts(scraper, proxy)

	if db.Set.ProxyAlreadyDead(scraper, proxy) {
		log.Debug().
			Str("scraper", scraper).
			Str("proxy", proxy).
			Msg("proxy already in DEAD")
		return
	}
	db.Set.Dead(scraper, proxy)

	var backOffTime int64
	if db.InProxyrack(proxy) {
		backOffTime = config.ProxyrackBackoffTime
	} else {
		backOffTime = expBackoffFullJitter(int(db.Base.FailedAttempts(scraper, proxy)))
	}
	db.Base.StoreNextCheck(scraper, proxy, now.Time()+backOffTime)
	log.Debug().
		Str("scraper", scraper).
		Str("proxy", proxy).
		Msg("proxy is DEAD")
}

func reanimateDead(scraper string) {
	var nReanimated int
	deadProxies := db.Set.GetDead(scraper)
	log.Info().
		Int("count", len(deadProxies)).
		Msg("Trying to reanimate dead proxies")
	for _, proxy := range deadProxies {
		nextCheck := db.Base.LoadNextCheck(scraper, proxy)
		if (nextCheck > 0) && (nextCheck <= now.Time()) {
			db.Base.CleanProxyInfo(scraper, proxy)
			nReanimated++
		}
	}
	if nReanimated > 0 {
		log.Info().
			Str("scraper", scraper).
			Int("count", nReanimated).
			Msg("proxies moved from 'dead' to 'unchecked'")
	}
}

func returnToGoodFromBusyAndPostponed(scraper string) {
	var nReturned int
	busyAndPostponed := db.Set.LoadBusyPostponed(scraper)
	for _, proxy := range busyAndPostponed {
		nextCheck := db.Base.LoadNextCheck(scraper, proxy)
		if nextCheck > 0 {
			if nextCheck <= now.Time() {
				if db.Set.ProxyInPostponed(scraper, proxy) {
					db.Base.CleanGoodAttempts(scraper, proxy)
				}
				db.Base.CleanNextCheck(scraper, proxy)
				db.Set.Good(scraper, proxy)
				nReturned++
			}
		}
	}
	if nReturned > 0 {
		log.Info().
			Str("scraper", scraper).
			Int("count", nReturned).
			Msg("proxies moved from 'busy' and 'postponed' to 'good'")
	}
}

// RemoveDeadProxiesForALongTime removes too old dead proxies
func RemoveDeadProxiesForALongTime(scraper string) {
	var nRemoved int
	var deadList []string
	deadProxies := db.Set.GetDead(scraper)
	for _, proxy := range deadProxies {
		proxyTime := db.Base.ProxyTime(scraper, proxy)
		timeToRemoveDeadProxies := proxyTime + config.RemoveDeadTime
		if proxyTime != 0 && timeToRemoveDeadProxies <= now.Time() {
			nRemoved++
			deadList = append(deadList, proxy)
		}
	}

	if nRemoved > 0 {
		db.Base.RemoveProxies(scraper, deadList)
		log.Info().
			Str("scraper", scraper).
			Int("count", nRemoved).
			Msg("proxies removed because they've been dead too long")
	}
}

func randomUniform(value float32) int64 {
	mn := 0.5 * value
	mx := 1.5 * value
	number := mn + (mx - mn)
	return int64(rand.Float32() * number)
}

func expBackoffFullJitter(attempts int) int64 {
	var capacity = 2592000.0
	var base = 128.0
	var backOffTime float32
	maxAttempts := int(math.Log(capacity/base) / math.Log(16))
	if attempts <= maxAttempts {
		backOffTime = float32(base * math.Pow(16.0, float64(attempts)))
	} else {
		backOffTime = float32(capacity)
	}
	return randomUniform(backOffTime)
}

// ReanimateProxies tries rise proxies from non-working state
func ReanimateProxies(scraper string) {
	reanimateDead(scraper)
	returnToGoodFromBusyAndPostponed(scraper)
	RemoveDeadProxiesForALongTime(scraper)
}
