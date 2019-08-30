package createblocks

import (
	"fmt"
	"testing"
)

const (
	Filename = "d:\\listdir\\list_test"
)

func createTestBlock() error {
	start := 0
	end := 0xf
	amountOfBlocks := 1
	amountOfThreads := 4
	bufferSize := int(1e6)

	_, err := Create(start, end, amountOfBlocks, amountOfThreads, bufferSize, Filename)
	if err != nil {
		return fmt.Errorf("could not create blocks with serial numbers %d through %d"+
			"with Filename %s: %v", start, end, Filename, err)
	}

	return nil
}

func TestReverseFileReader_Refill(t *testing.T) {
	createTestBlock()

	reader := NewReverseFileReader(0, Filename, 1000)
	if err := reader.Refill(); err != nil {
		t.Errorf(err.Error())
	}
}

/*
func TestReverseFileReader_Peek(t *testing.T) {
	createTestBlock()

	reader := NewReverseFileReader(0, Filename)
	if err := reader.Refill(); err != nil {
		t.Errorf(err.Error())
	}

	digest := reader.Peek()
}

*/
