package model

import (
	"crypto/md5"
	"encoding/hex"
	"testing"
)

func TestCreateSubBlock(t *testing.T) {
	/*
		Test normal case
	*/
	start := 0
	end := 10
	b := Block{0, start, end, make([]HashDigest, end-start+1)}
	finished := make(chan error)

	go b.CreateSubBlock(start, end, start, finished)

	err := <-finished
	if err != nil {
		t.Errorf("received error over chan from Block.createSubBlock: %+v", err)
	}
	if len(b.Hashes) != end-start+1 {
		t.Errorf("length of created hashes in Block.createSubBlock is incorrect: "+
			"expected len=%d, got len=%d", end-start+1, len(b.Hashes))
	}

	expectedHashDigests := getExpectedHashDigests()
	for i := 0; i < len(b.Hashes); i++ {
		if b.Hashes[i] != expectedHashDigests[i] {
			t.Errorf("Block.createSubBlock produced incorrect hashes in range %d through %d (i=%d)", start, end, i)
			break
		}
	}

	/*
		Test []HashDigest to small, doesn't fit all vals, should receive error
	*/
	b = Block{0, start, end, make([]HashDigest, end-start+1-3)}
	go b.CreateSubBlock(start, end, start, finished)
	err = <-finished
	if err == nil {
		t.Errorf("did not received error over chan from Block.createSubBlock: %+v", err)
	}

}

func getExpectedHashDigests() []HashDigest {
	expectedHashes := []string{
		"9f9a6db9e2b3c0a5c03984eed1adf9f6",
		"18e981ca082b9725af58098c457a6836",
		"2493c58d2d2f97be7eabf4a2eb842335",
		"a3eef34cd06e0cd15ad20ed2edeac9c1",
		"17a0046db44caba022f0811490b8bf6b",
		"56586fb8296e3fd28302b8005ad6415d",
		"c8ccc5501d6fbbc926c0328ae7287b7f",
		"6dc565a0c9da7d4fde332defc3d40603",
		"deac643a731dc1820438d7d779a57023",
		"736754e20944459aab5326897f3d3f1b",
		"15e0b103b82b8ab3bbf58f8059ad489d",
	}
	expectedHashDigests := make([]HashDigest, len(expectedHashes))
	var tmpBytes []byte
	var tmpHashDigest [md5.Size]byte
	for i := 0; i < len(expectedHashes); i++ {
		tmpBytes, _ = hex.DecodeString(expectedHashes[i])
		copy(tmpHashDigest[:], tmpBytes)
		expectedHashDigests[i] = tmpHashDigest
	}
	return expectedHashDigests
}
