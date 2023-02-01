package main

import (
	"context"
	"crypto/tls"
	"net/http"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/ipsl/unixfs"
	"github.com/ipfs/go-libipfs/rapide"
	"github.com/ipfs/go-libipfs/rapide/gateway"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const useHttp2 = false

	var client *http.Client
	if useHttp2 {
		client = http.DefaultClient
	} else {
		transport := &http.Transport{
			TLSNextProto: make(map[string]func(string, *tls.Conn) http.RoundTripper), // Disable HTTP/2
		}
		client = &http.Client{
			Transport: transport,
		}
	}

	r := rapide.Client{ServerDrivenDownloaders: []rapide.ServerDrivenDownloader{
		gateway.Gateway{Name: "ipfs.io 1", PathName: "https://ipfs.io/ipfs/", Client: client},
		gateway.Gateway{Name: "strn.pl 1", PathName: "https://strn.pl/ipfs/", Client: client},
		gateway.Gateway{Name: "cf-ipfs.com 1", PathName: "https://cf-ipfs.com/ipfs/", Client: client},
		gateway.Gateway{Name: "jorropo.net 1", PathName: "https://jorropo.net/ipfs/", Client: client},
		gateway.Gateway{Name: "ipfs.io 2", PathName: "https://ipfs.io/ipfs/", Client: client},
		gateway.Gateway{Name: "strn.pl 2", PathName: "https://strn.pl/ipfs/", Client: client},
		gateway.Gateway{Name: "cf-ipfs.com 2", PathName: "https://cf-ipfs.com/ipfs/", Client: client},
		gateway.Gateway{Name: "jorropo.net 2", PathName: "https://jorropo.net/ipfs/", Client: client},
		gateway.Gateway{Name: "ipfs.io 3", PathName: "https://ipfs.io/ipfs/", Client: client},
		gateway.Gateway{Name: "strn.pl 3", PathName: "https://strn.pl/ipfs/", Client: client},
		gateway.Gateway{Name: "cf-ipfs.com 3", PathName: "https://cf-ipfs.com/ipfs/", Client: client},
		gateway.Gateway{Name: "jorropo.net 3", PathName: "https://jorropo.net/ipfs/", Client: client},
	}}

	start := time.Now()
	lastTime := start
	var i uint64
	var last uint64

	const distipfsio = "QmfQYLz4gf4oXLKFuG1aL9Z7jhkf1yBAii1L7oDRhW2ZZR"
	const ipfsio = "QmZJXE2Q3Fccxt3XLiqv9x7MgpQHxZkeqFKKjvAPhqa3Ht"

	for e := range r.Get(ctx, cid.MustParse(distipfsio), unixfs.Everything()) {
		b, err := e.Get()
		if err != nil {
			panic(err.Error())
		}
		i += uint64(len(b.RawData()))
		if i > last+1024*1024*128 {
			now := time.Now()
			rapide.Println(float64(i-last)/now.Sub(lastTime).Seconds()/(1024*1024), "MiB/s")
			lastTime = now
			last = i
		}
	}
	dur := time.Since(start)
	rapide.Println("Total:", i, "bytes", dur, float64(i)/dur.Seconds()/(1024*1024), "MiB/s")
}
