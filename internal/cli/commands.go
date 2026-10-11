// Minimal CLI usage text.
package cli

import "fmt"

func Usage() {
	fmt.Println("p2cache put <file> | get <hash> <output> | peers | stats | serve")
}
