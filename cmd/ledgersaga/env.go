package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

func env(k, def string) string {
	if v, ok := os.LookupEnv(k); ok {
		return v
	}
	return def
}

func envOr[T any](k string, def T, parse func(string) (T, error)) T {
	v, ok := os.LookupEnv(k)
	if !ok {
		return def
	}
	x, err := parse(v)
	if err != nil {
		panic(fmt.Sprintf("%s=%q: %v", k, v, err))
	}
	return x
}

func envInt(k string, def int) int { return envOr(k, def, strconv.Atoi) }
func envDur(k string, def time.Duration) time.Duration {
	return envOr(k, def, time.ParseDuration)
}
func envFloat(k string, def float64) float64 {
	return envOr(k, def, func(s string) (float64, error) { return strconv.ParseFloat(s, 64) })
}
