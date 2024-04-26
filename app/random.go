package main

import (
	"math/rand"
	"time"
)

const alphanumericChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func generateRandomString(length int) string {
	var seededRand *rand.Rand = rand.New(
		rand.NewSource(time.Now().UnixNano()))
	bytes := make([]byte, length)
	for i := range bytes {
		bytes[i] = alphanumericChars[seededRand.Intn(len(alphanumericChars))]
	}
	return string(bytes)
}
