package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"time"
)

type Request struct {
	Id       string
	LatencyC int
	LatencyA int
	LatencyB int
	ResultA  string
	ResultB  string
	ResultC  string
}

var avgExecutionRequest [1000]int64

func LoadRequests(filename string) {
	f, _ := os.Open(filename)
	defer f.Close()
	reader := csv.NewReader(f)
	for row, err := reader.Read(); err == nil; row, err = reader.Read() {
		latencyA, _ := strconv.Atoi(row[1])
		latencyB, _ := strconv.Atoi(row[2])
		latencyC, _ := strconv.Atoi(row[3])
		request := &Request{
			Id:       row[0],
			LatencyC: latencyC,
			LatencyA: latencyA,
			LatencyB: latencyB,
		}
		go Execute(request)
	}
}

func IsClosed(ch <-chan string) bool {
	select {
	case <-ch:
		return true
	default:
	}

	return false
}

func Execute(request *Request) {
	t1 := time.Now()
	channelA := make(chan string)
	channelB := make(chan string)
	channelC := make(chan string)
	defer close(channelA)
	defer close(channelB)
	defer close(channelC)
	go func(req Request) {
		time.Sleep(time.Duration(req.LatencyA) * time.Millisecond)
		if !IsClosed(channelA) {
			channelA <- "Result Operation A"
		}
	}(*request)
	go func(req Request) {
		time.Sleep(time.Duration(req.LatencyB) * time.Millisecond)
		if !IsClosed(channelB) {
			channelB <- "Result Operation B"
		}
	}(*request)
	go func(req Request) {
		time.Sleep(time.Duration(req.LatencyC) * time.Millisecond)
		if !IsClosed(channelC) {
			channelB <- "Result Operation C"
		}
	}(*request)
	for i := 0; i < 3; i++ {
		select {
		case resultA := <-channelA:
			request.ResultA = resultA
		case resultB := <-channelB:
			request.ResultB = resultB
		case resultC := <-channelC:
			request.ResultC = resultC
		case <-time.After(20 * time.Millisecond):
			break
		}
	}
	t2 := time.Now()
	diff := t2.Sub(t1)
	idx, _ := strconv.Atoi(request.Id)
	avgExecutionRequest[idx-1] = diff.Milliseconds()
}

func main() {
	LoadRequests("requests-lowlatency.csv")
	fmt.Print("Press Enter to finish...")
	fmt.Scanln()
	total := int64(0)
	for _, v := range avgExecutionRequest {
		total += v
	}
	fmt.Printf("Average latency: %2.2fms\n", float64(total/1000))
}
