package record

import "io"

// mergeItem is an interface required by the merge function
type mergeItem interface {
	// Next moves to the Next item
	next()
	// Write writes the current item
	write(io.Writer)
	// Key returns the Key of the current item
	key() *string
}

func Merge(target io.Writer, maxScanTokenSize int, sources ...io.Reader) error {
	coalesceKey := func(mI mergeItem) *string {
		if mI != nil {
			return mI.key()
		}

		return nil
	}

	items := []*scannerCursor{}
	for _, source := range sources {
		scanner, err := NewScanner(source, maxScanTokenSize)
		if err != nil {
			return err
		}
		item := newScannerCursor(scanner)
		items = append(items, item)
	}

	var minPtr mergeItem
	var lastWrittenKey *string
	var scannerItems []mergeItem
	for _, item := range items {
		item.next()
		scannerItems = append([]mergeItem{item}, scannerItems...)
	}

	for {
		for _, curItem := range scannerItems {
			minKey := coalesceKey(minPtr)
			curKey := coalesceKey(curItem)
			if minKey == nil || (minKey != nil && curKey != nil && *curKey < *minKey) {
				minPtr = curItem
			}
		}

		if coalesceKey(minPtr) != nil {
			if lastWrittenKey == nil || (minPtr.key() != nil && *lastWrittenKey != *minPtr.key()) {
				minPtr.write(target)
				lastWrittenKey = minPtr.key()
			}

			minPtr.next()
			minPtr = nil
		} else {
			break
		}
	}

	return nil
}
