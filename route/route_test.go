package route

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestDecompression(t *testing.T) {
	payload := "payload"
	pReader := strings.NewReader(payload)

	decoders, err := makeDecoders(numZstdDecoders)
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}

	router := &Router{zstdDecoders: decoders}
	req := &http.Request{
		Body:   ioutil.NopCloser(pReader),
		Header: http.Header{},
	}
	reader, err := router.getMaybeCompressedBody(req)
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}

	b, err := ioutil.ReadAll(reader)
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}
	if string(b) != payload {
		t.Errorf("%s != %s", string(b), payload)
	}

	buf := &bytes.Buffer{}
	w := gzip.NewWriter(buf)
	_, err = w.Write([]byte(payload))
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}
	w.Close()

	req.Body = ioutil.NopCloser(buf)
	req.Header.Set("Content-Encoding", "gzip")
	reader, err = router.getMaybeCompressedBody(req)
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}

	b, err = ioutil.ReadAll(reader)
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}
	if string(b) != payload {
		t.Errorf("%s != %s", string(b), payload)
	}

	buf = &bytes.Buffer{}
	zstdW, err := zstd.NewWriter(buf)
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}
	_, err = zstdW.Write([]byte(payload))
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}
	zstdW.Close()

	req.Body = ioutil.NopCloser(buf)
	req.Header.Set("Content-Encoding", "zstd")
	reader, err = router.getMaybeCompressedBody(req)
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}

	b, err = ioutil.ReadAll(reader)
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}
	if string(b) != payload {
		t.Errorf("%s != %s", string(b), payload)
	}
}
