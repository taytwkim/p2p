package main

import (
	"bytes"
	"io"
	"testing"
)

func TestTransferResponseHeaderDoesNotLeakDelimiterIntoBody(t *testing.T) {
	var stream bytes.Buffer
	body := []byte("{manifest-json-starts-like-this}")

	if err := writeTransferResponseHeader(&stream, TransferResponse{
		Kind:     string(ObjectManifest),
		Filesize: int64(len(body)),
		Filename: "manifest.json",
	}); err != nil {
		t.Fatal(err)
	}
	stream.Write(body)

	var resp TransferResponse
	bodyReader, err := readTransferResponseHeader(&stream, &resp)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Kind != string(ObjectManifest) {
		t.Fatalf("kind = %q, want %q", resp.Kind, ObjectManifest)
	}

	got, err := io.ReadAll(io.LimitReader(bodyReader, resp.Filesize))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("body = %q, want %q", got, body)
	}
}
