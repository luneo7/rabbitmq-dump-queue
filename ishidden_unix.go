// +build !windows

package main

import (
	"log"
	"runtime"
)

func isHidden(filename string) (bool, error) {
	if runtime.GOOS != "windows" {
		if filename[0:1] == "." {
			return true, nil
		}

		return false, nil
	}

	log.Fatal("Unable to check if file is hidden under this OS")
	return false, nil
}
