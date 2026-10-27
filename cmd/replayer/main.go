package main

import (
	"os"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/app/replayer"
)

func main() { os.Exit(replayer.Run()) }
