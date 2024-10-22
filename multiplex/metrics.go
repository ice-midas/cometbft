package multiplex

import (
	"time"

	"github.com/cometbft/cometbft/libs/metrics"
)

// addTimeSample returns a function that, when called, adds an observation to m.
// The observation added to m is the number of seconds elapsed since start.
// addTimeSample is meant to be called in a defer to calculate the amount of
// time a function takes to complete.
func addTimeSample(m metrics.Histogram, start time.Time) func() {
	return func() { m.Observe(time.Since(start).Seconds()) }
}

// addTimeSampleNow returns a function that, when called, adds an observation to m.
// The observation added to m is the number of seconds elapsed since addTimeSampleNow
// was initially called. addTimeSampleNow is meant to be called in a defer to calculate
// the amount of time a function takes to complete.
func addTimeSampleNow(m metrics.Histogram) func() {
	start := time.Now()
	return func() { m.Observe(time.Since(start).Seconds()) }
}
