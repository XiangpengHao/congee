# Performance comparison of Congee Flat Columnar vs Congee Flat Struct vs Congee Set

Input size: 16015 key value pairs of which most keys are sequential in nature.

Congee set stats: 
╭────────────────────────────────────────────────────────────────────────────────────────────────╮
│                                        Congee ART Statistics                                   │
├────────────────────────────────────────────────────────────────────────────────────────────────┤
│ L 0 │ N4:    1( 0.8) │ N16:    0( 0.0) │ N48:    0( 0.0) │ N256:    0( 0.0) │             56 B │
│ L 1 │ N4:    0( 0.0) │ N16:    2( 0.5) │ N48:    0( 0.0) │ N256:    1( 0.2) │           2.4 KB │
│ L 2 │ N4:    0( 0.0) │ N16:    0( 0.0) │ N48:    0( 0.0) │ N256:   63( 1.0) │         129.0 KB │
├────────────────────────────────────────────────────────────────────────────────────────────────┤
│ Total Memory:   131.4 KB │ Nodes:       67 │ Entries:    16081 │ KV Pairs:    16015            │
│ Load Factor: 0.96       │ N4: 56 B     │ N16: 320 B    │ N48: 0 B      │ N256: 131.0 KB        │
╰────────────────────────────────────────────────────────────────────────────────────────────────╯


node_types len: 67 size: 67
prefix_bytes len: 17 size: 17
prefix_offsets len: 67 size: 268
children_data len: 16081 size: 64324
children_offsets len: 67 size: 268
total size: 64944
nodes len: 67
Size of congee flat: 65012
 *** congee flat ***

Searching for key: [1, 1, 2, 3, 4, 5, 6, 1]
Found: true in 2µs

Searching for key 2: [2, 1, 2, 3, 4, 5, 6, 8]
Found: true in 1.083µs

Searching for key: [1, 1, 2, 3, 4, 5, 6, 99]
Found: false in 875ns

Searching for key: [0, 0, 0, 0, 0, 0, 0, 114]
Found: true in 2.084µs

Searching for key: [0, 0, 0, 0, 0, 0, 0, 115]
Found: true in 2µs

Searching for key: [0, 0, 0, 0, 0, 0, 0, 116]
Found: true in 1.916µs

Searching for key: [0, 0, 0, 0, 0, 0, 0, 117]
Found: true in 1.958µs

Searching for key: [0, 0, 0, 0, 0, 0, 0, 118]
Found: true in 1.916µs

Searching for key: [0, 0, 0, 0, 0, 0, 0, 119]
Found: true in 2.042µs

Searching for key: [0, 0, 0, 0, 0, 0, 0, 120]
Found: true in 2.042µs

Searching for key: [0, 0, 0, 0, 0, 0, 0, 121]
Found: true in 2.125µs

Searching for key: [0, 0, 0, 0, 0, 0, 0, 122]
Found: true in 2µs

Searching for key: [0, 0, 0, 0, 0, 0, 0, 123]
Found: true in 2.083µs
 *** congee flat struct ***
nodes len: 67
Size of congee flat struct: 66260
 *** congee flat struct ***

Searching for key: [1, 1, 2, 3, 4, 5, 6, 1]
Found: true in 2.916µs

Searching for key 2: [2, 1, 2, 3, 4, 5, 6, 8]
Found: true in 1.541µs

Searching for key: [1, 1, 2, 3, 4, 5, 6, 99]
Found: false in 1.417µs

Searching for key: [0, 0, 0, 0, 0, 0, 3, 246]
Found: true in 3.709µs

Searching for key: [0, 0, 0, 0, 0, 0, 3, 247]
Found: true in 2.5µs

Searching for key: [0, 0, 0, 0, 0, 0, 3, 248]
Found: true in 2.292µs

Searching for key: [0, 0, 0, 0, 0, 0, 3, 249]
Found: true in 2.583µs

Searching for key: [0, 0, 0, 0, 0, 0, 3, 250]
Found: true in 2.417µs

Searching for key: [0, 0, 0, 0, 0, 0, 3, 251]
Found: true in 2.625µs

Searching for key: [0, 0, 0, 0, 0, 0, 3, 252]
Found: true in 2.5µs

Searching for key: [0, 0, 0, 0, 0, 0, 3, 253]
Found: true in 2.584µs

Searching for key: [0, 0, 0, 0, 0, 0, 3, 254]
Found: true in 2.458µs

Searching for key: [0, 0, 0, 0, 0, 0, 3, 255]
Found: true in 2.458µs
 *** congee set ***
Size of CongeeSet: 134520 bytes

Searching for key: [1, 1, 2, 3, 4, 5, 6, 1]
Found: true in 1.333µs

Searching for key: [2, 1, 2, 3, 4, 5, 6, 8]
Found: true in 583ns

Searching for key: [1, 1, 2, 3, 4, 5, 6, 99]
Found: false in 1.166µs

Searching for key: [0, 0, 0, 0, 0, 0, 0, 114]
Found: true in 625ns

Searching for key: [0, 0, 0, 0, 0, 0, 0, 115]
Found: true in 416ns

Searching for key: [0, 0, 0, 0, 0, 0, 0, 116]
Found: true in 417ns

Searching for key: [0, 0, 0, 0, 0, 0, 0, 117]
Found: true in 416ns

Searching for key: [0, 0, 0, 0, 0, 0, 0, 118]
Found: true in 459ns

Searching for key: [0, 0, 0, 0, 0, 0, 0, 119]
Found: true in 416ns

Searching for key: [0, 0, 0, 0, 0, 0, 0, 120]
Found: true in 416ns

Searching for key: [0, 0, 0, 0, 0, 0, 0, 121]
Found: true in 417ns

Searching for key: [0, 0, 0, 0, 0, 0, 0, 122]
Found: true in 458ns

Searching for key: [0, 0, 0, 0, 0, 0, 0, 123]
Found: true in 417ns


*** memory comparison ***

## Overall memory savings

CongeeSet memory: 134520 bytes
CongeeFlat memory: 65012 bytes - 51.7% reduction (2.07x smaller)
CongeeFlatStruct memory: 66260 bytes - 50.7% reduction (2.03x smaller)

*** performance tests ***
CongeeFlat: 10000000 contains() calls in 17.863950625s 
CongeeFlatStruct: 10000000 contains() calls in 23.000939667s
CongeeSet: 10000000 contains() calls in 3.661694916s


## Performance Analysis

