package main

import (
	"bytes"
	"fmt"
	"github.com/pierrec/lz4"
	"io"
	"strings"
	"testing"
)

func TestLz4(t *testing.T) {
	//buffer := bytes.NewBuffer([]byte{})
	//writer := lz4.NewWriter(buffer)
	//n, err := writer.Write([]byte("hello world"))
	//if err != nil {
	//	panic(err)
	//}
	//writer.Close()
	//fmt.Println("first:", n)
	//fmt.Println(buffer.Bytes())

	buffer := bytes.NewBuffer([]byte{4, 34, 77, 24, 100, 112, 185})
	reader := lz4.NewReader(buffer)
	b := make([]byte, 88000)
	reader.Read(b)
	fmt.Println(b)

}

func TestLz4Read(t *testing.T) {

	// Compress and uncompress an input string.
	s := "hello world"
	r := strings.NewReader(s)

	// The pipe will uncompress the data from the writer.
	pr, pw := io.Pipe()
	zw := lz4.NewWriter(pw)
	//zr := lz4.NewReader(pr)

	go func() {

		// Compress the input string.
		_, _ = io.Copy(zw, r)
		_ = zw.Close() // Make sure the writer is closed
		_ = pw.Close() // Terminate the pipe
	}()

	all, err := io.ReadAll(pr)
	if err != nil {
		panic(err)
	}
	fmt.Println(all)
	decode(bytes.NewReader(all))

}

func decode(all io.Reader) {
	buffer := bytes.NewBuffer([]byte{})
	_, _ = io.Copy(buffer, lz4.NewReader(all))
	fmt.Println(buffer.Bytes(), string(buffer.Bytes()))
}
