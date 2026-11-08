# preflate-rs (vendored)

The `preflate` crate of [microsoft/preflate-rs](https://github.com/microsoft/preflate-rs), Apache-2.0,
from branch `restart-points` at d467a54. On top of upstream:

- accept a single one-bit Huffman code (Go's encoder; upstream PR #78)
- a model of Go's compress/flate Huffman trees and block size
- independently rebuildable restart-point groups, rebuilt one at a time and resumable

Files changed from upstream `e97efb0` say so in their first line.

Refresh with `scripts/vendor-preflate.sh`.
