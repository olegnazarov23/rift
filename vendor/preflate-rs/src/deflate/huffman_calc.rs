// Changed for rift; see vendor/preflate-rs/README.md.
/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the Apache License, Version 2.0. See LICENSE.txt in the project root for license information.
 *  This software incorporates material from third parties. See NOTICE.txt for details.
 *--------------------------------------------------------------------------------------------*/

use bitcode::{Decode, Encode};

#[allow(dead_code)]
#[derive(Encode, Decode, Copy, Clone, Debug, Eq, PartialEq)]
pub enum HufftreeBitCalc {
    Zlib,
    Miniz,
    Go,
}

/// Calculates the Huffman bit lengths for a given distribution of symbols.
///
/// There is no one-size-fits-all solution for calculating Huffman bit lengths and
/// each library has its own.
///
/// If we can get the right algorithm, it will minimize the amount of corrections
/// that we later need to write when encoding the data back to the compressed format.
pub fn calc_bit_lengths(
    bit_calc: HufftreeBitCalc,
    sym_count: &[u16],
    code_size_limit: usize,
) -> Vec<u8> {
    match bit_calc {
        HufftreeBitCalc::Zlib => calc_zlib::calc_bit_lengths(sym_count, code_size_limit),
        HufftreeBitCalc::Miniz => calc_minzoxide::calc_bit_lengths(sym_count, code_size_limit),
        HufftreeBitCalc::Go => calc_go::calc_bit_lengths(sym_count, code_size_limit),
    }
}

/// Port of Go's compress/flate huffman_code.go (generate, bitCounts, assignEncodingAndSize).
mod calc_go {
    const MAX_BITS_LIMIT: usize = 16;

    #[derive(Clone, Copy, Default)]
    struct Level {
        last_freq: i32,
        next_char_freq: i32,
        next_pair_freq: i32,
        needed: i32,
    }

    pub fn calc_bit_lengths(sym_count: &[u16], max_bits: usize) -> Vec<u8> {
        let mut list: Vec<(u16, i32)> = sym_count
            .iter()
            .enumerate()
            .filter(|(_, f)| **f != 0)
            .map(|(i, &f)| (i as u16, i32::from(f)))
            .collect();
        if list.is_empty() {
            list.push((0, 1)); // Go forces one distance code for blocks without matches
        }
        let mut lengths = vec![0u8; usize::from(list.last().unwrap().0) + 1];
        if list.len() <= 2 {
            for &(sym, _) in &list {
                lengths[usize::from(sym)] = 1;
            }
            return lengths;
        }
        list.sort_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));

        // the most frequent symbols (end of the list) get the shortest codes
        let mut end = list.len();
        for (bits, &count) in bit_counts(&list, max_bits).iter().enumerate().skip(1) {
            for &(sym, _) in &list[end - count as usize..end] {
                lengths[usize::from(sym)] = bits as u8;
            }
            end -= count as usize;
        }
        lengths
    }

    fn bit_counts(list: &[(u16, i32)], max_bits: usize) -> Vec<i32> {
        let n = list.len() as i32;
        let freq = |i: i32| list.get(i as usize).map_or(i32::MAX, |e| e.1);
        let max_bits = max_bits.min(list.len() - 1);
        let mut levels = [Level::default(); MAX_BITS_LIMIT + 1];
        let mut leaf_counts = [[0i32; MAX_BITS_LIMIT + 1]; MAX_BITS_LIMIT + 1];
        for level in 1..=max_bits {
            levels[level] = Level {
                last_freq: freq(1),
                next_char_freq: freq(2),
                next_pair_freq: if level == 1 {
                    i32::MAX
                } else {
                    freq(0) + freq(1)
                },
                needed: 0,
            };
            leaf_counts[level][level] = 2;
        }
        levels[max_bits].needed = 2 * n - 4;

        let mut level = max_bits;
        loop {
            if levels[level].next_pair_freq == i32::MAX && levels[level].next_char_freq == i32::MAX
            {
                levels[level].needed = 0;
                levels[level + 1].next_pair_freq = i32::MAX;
                level += 1;
                continue;
            }
            let prev_freq = levels[level].last_freq;
            if levels[level].next_char_freq < levels[level].next_pair_freq {
                let next = leaf_counts[level][level] + 1;
                levels[level].last_freq = levels[level].next_char_freq;
                leaf_counts[level][level] = next;
                levels[level].next_char_freq = freq(next);
            } else {
                levels[level].last_freq = levels[level].next_pair_freq;
                let below = leaf_counts[level - 1];
                leaf_counts[level][..level].copy_from_slice(&below[..level]);
                levels[level - 1].needed = 2;
            }
            levels[level].needed -= 1;
            if levels[level].needed == 0 {
                if level == max_bits {
                    break;
                }
                levels[level + 1].next_pair_freq = prev_freq.wrapping_add(levels[level].last_freq);
                level += 1;
            } else {
                while levels[level - 1].needed > 0 {
                    level -= 1;
                }
            }
        }

        let counts = &leaf_counts[max_bits];
        let mut bit_count = vec![0i32; max_bits + 1];
        for (i, level) in (1..=max_bits).rev().enumerate() {
            bit_count[i + 1] = counts[level] - counts[level - 1];
        }
        bit_count
    }
}

mod calc_minzoxide {
    use std::mem;

    const MAX_SUPPORTED_HUFF_CODESIZE: usize = 32;

    /// calculates the bit lengths for a given distribution of symbols.
    /// Trailing zeros are removed and the maximum code size is enforced.
    pub fn calc_bit_lengths(sym_count: &[u16], code_size_limit: usize) -> Vec<u8> {
        let mut symbols0 = Vec::new();
        let mut max_used = 0;

        for i in 0..sym_count.len() {
            if sym_count[i] != 0 {
                symbols0.push(SymFreq {
                    key: sym_count[i],
                    sym_index: i as u16,
                });
                max_used = i + 1;
            }
        }

        let num_used_symbols = symbols0.len();

        let mut symbols1 = Vec::new();
        symbols1.resize(
            num_used_symbols,
            SymFreq {
                key: 0,
                sym_index: 0,
            },
        );

        let symbols = radix_sort_symbols(&mut symbols0[..], &mut symbols1[..]);
        calculate_minimum_redundancy(symbols);

        let mut num_codes = [0i32; MAX_SUPPORTED_HUFF_CODESIZE + 1];
        for symbol in symbols.iter() {
            num_codes[symbol.key as usize] += 1;
        }

        enforce_max_code_size(&mut num_codes, num_used_symbols, code_size_limit);

        let mut code_sizes = vec![0; max_used];

        let mut last = num_used_symbols;
        for (i, &num_item) in num_codes
            .iter()
            .enumerate()
            .take(code_size_limit + 1)
            .skip(1)
        {
            let first = last - num_item as usize;
            for symbol in &symbols[first..last] {
                code_sizes[symbol.sym_index as usize] = i as u8;
            }
            last = first;
        }

        code_sizes
    }

    #[derive(Copy, Clone)]
    struct SymFreq {
        key: u16,
        sym_index: u16,
    }

    fn radix_sort_symbols<'a>(
        symbols0: &'a mut [SymFreq],
        symbols1: &'a mut [SymFreq],
    ) -> &'a mut [SymFreq] {
        let mut hist = [[0; 256]; 2];

        for freq in symbols0.iter() {
            hist[0][(freq.key & 0xFF) as usize] += 1;
            hist[1][((freq.key >> 8) & 0xFF) as usize] += 1;
        }

        let mut n_passes = 2;
        if symbols0.len() == hist[1][0] {
            n_passes -= 1;
        }

        let mut current_symbols = symbols0;
        let mut new_symbols = symbols1;

        for (pass, hist_item) in hist.iter().enumerate().take(n_passes) {
            let mut offsets = [0; 256];
            let mut offset = 0;
            for i in 0..256 {
                offsets[i] = offset;
                offset += hist_item[i];
            }

            for sym in current_symbols.iter() {
                let j = ((sym.key >> (pass * 8)) & 0xFF) as usize;
                new_symbols[offsets[j]] = *sym;
                offsets[j] += 1;
            }

            mem::swap(&mut current_symbols, &mut new_symbols);
        }

        current_symbols
    }

    fn calculate_minimum_redundancy(symbols: &mut [SymFreq]) {
        match symbols.len() {
            0 => (),
            1 => symbols[0].key = 1,
            n => {
                symbols[0].key += symbols[1].key;
                let mut root = 0;
                let mut leaf = 2;
                for next in 1..n - 1 {
                    if (leaf >= n) || (symbols[root].key < symbols[leaf].key) {
                        symbols[next].key = symbols[root].key;
                        symbols[root].key = next as u16;
                        root += 1;
                    } else {
                        symbols[next].key = symbols[leaf].key;
                        leaf += 1;
                    }

                    if (leaf >= n) || (root < next && symbols[root].key < symbols[leaf].key) {
                        symbols[next].key = symbols[next].key.wrapping_add(symbols[root].key);
                        symbols[root].key = next as u16;
                        root += 1;
                    } else {
                        symbols[next].key = symbols[next].key.wrapping_add(symbols[leaf].key);
                        leaf += 1;
                    }
                }

                symbols[n - 2].key = 0;
                for next in (0..n - 2).rev() {
                    symbols[next].key = symbols[symbols[next].key as usize].key + 1;
                }

                let mut avbl = 1;
                let mut used = 0;
                let mut dpth = 0;
                let mut root = (n - 2) as i32;
                let mut next = (n - 1) as i32;
                while avbl > 0 {
                    while (root >= 0) && (symbols[root as usize].key == dpth) {
                        used += 1;
                        root -= 1;
                    }
                    while avbl > used {
                        symbols[next as usize].key = dpth;
                        next -= 1;
                        avbl -= 1;
                    }
                    avbl = 2 * used;
                    dpth += 1;
                    used = 0;
                }
            }
        }
    }

    fn enforce_max_code_size(num_codes: &mut [i32], code_list_len: usize, max_code_size: usize) {
        if code_list_len <= 1 {
            return;
        }

        num_codes[max_code_size] += num_codes[max_code_size + 1..].iter().sum::<i32>();
        let total = num_codes[1..=max_code_size]
            .iter()
            .rev()
            .enumerate()
            .fold(0u32, |total, (i, &x)| total + ((x as u32) << i));

        for _ in (1 << max_code_size)..total {
            num_codes[max_code_size] -= 1;
            for i in (1..max_code_size).rev() {
                if num_codes[i] != 0 {
                    num_codes[i] -= 1;
                    num_codes[i + 1] += 2;
                    break;
                }
            }
        }
    }

    /// verify that the huffman codes generated can be decoded with the huffman code tree
    #[test]
    fn roundtrip_huffman_code() {
        // requires overflow treatment
        let frequencies = [
            1, 0, 1, 0, 9, 9, 8, 19, 36, 70, 73, 34, 5, 6, 0, 0, 11, 1, 0,
        ];
        let code_lengths = calc_bit_lengths(&frequencies, 7);
        assert_eq!(
            code_lengths[..],
            [7, 0, 7, 0, 5, 5, 5, 4, 3, 2, 2, 3, 7, 5, 0, 0, 5, 7]
        );

        let frequencies = [2, 0, 0, 0, 8, 7, 4, 5, 54, 32, 4, 6, 3, 1, 2, 0, 34, 0, 0];
        let code_lengths = calc_bit_lengths(&frequencies, 7);
        assert_eq!(
            code_lengths[..],
            [7, 0, 0, 0, 4, 5, 6, 5, 2, 2, 5, 5, 6, 7, 6, 0, 2]
        );
    }
}

mod calc_zlib {
    #[derive(Copy, Clone, Debug)]
    enum HuffTree {
        Leaf(usize),
        Node { left: usize, right: usize },
    }

    #[derive(Copy, Clone, Debug)]
    struct HuffTreeNode {
        freq: u32,
        depth: u32,
        tree: HuffTree,
    }

    impl Eq for HuffTreeNode {}

    impl PartialEq for HuffTreeNode {
        fn eq(&self, other: &Self) -> bool {
            self.freq == other.freq
        }
    }

    impl Ord for HuffTreeNode {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            let r = other.freq.cmp(&self.freq);
            if r == std::cmp::Ordering::Equal {
                other.depth.cmp(&self.depth)
            } else {
                r
            }
        }
    }

    impl PartialOrd for HuffTreeNode {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }

    /// Restore the heap property by moving down the tree starting at node k.
    /// Exchange a node with the smallest of its two sons if necessary.
    /// Stop when the heap property is re-established (each father smaller than its two sons).
    fn pqdownheap(heap: &mut Vec<HuffTreeNode>, mut root: usize) {
        /// Compares two subtrees using the tree depth as a tie-breaker when frequencies are equal.
        fn smaller(n: &HuffTreeNode, m: &HuffTreeNode) -> bool {
            n.freq < m.freq || (n.freq == m.freq && n.depth <= m.depth)
        }

        let mut child = 2 * root + 1;

        let v = heap[root];

        while child < heap.len() {
            // Set j to the smallest of the two sons:
            if child + 1 < heap.len() && smaller(&heap[child + 1], &heap[child]) {
                child += 1;
            }

            // Exit if v is smaller than both sons
            if smaller(&v, &heap[child]) {
                break;
            }

            heap[root] = heap[child];
            root = child;

            // And continue down the tree, setting j to the left son of k
            child = 2 * root + 1;
        }

        heap[root] = v;
    }

    const SMALLEST: usize = 0;

    pub fn calc_bit_lengths(sym_freq: &[u16], max_bits: usize) -> Vec<u8> {
        // construct binary heap so that we can get out the frequencies in ascending order
        let mut heap = Vec::new();
        let mut max_code = 0;

        for (index, &freq) in sym_freq.iter().enumerate() {
            if freq > 0 {
                heap.push(HuffTreeNode {
                    freq: freq as u32,
                    depth: 0,
                    tree: HuffTree::Leaf(index),
                });
                max_code = index;
            }
        }

        let mut node_bit_len = vec![0u8; max_code + 1];

        if heap.len() <= 1 {
            // only one symbol, so give it a bit length of 1, plus some other random symbol
            // in order to ensure we have a valid tree
            node_bit_len[max_code] = 1;

            if max_code != 0 {
                node_bit_len[0] = 1;
            } else {
                node_bit_len.push(1);
            }

            return node_bit_len;
        }

        let mut n = heap.len() / 2;
        while n >= 1 {
            n -= 1;
            pqdownheap(&mut heap, n);
        }

        let mut nodes = Vec::new();

        loop {
            // get the two smallest frequencies and combine them into one new node
            let least1 = heap[SMALLEST];
            heap[SMALLEST] = heap.pop().unwrap();
            pqdownheap(&mut heap, SMALLEST);

            let least2 = heap[SMALLEST];

            let sum_freq = least1.freq + least2.freq;
            let sum_depth = std::cmp::max(least1.depth, least2.depth) + 1;

            nodes.push(least1);
            nodes.push(least2);

            let node = HuffTreeNode {
                freq: sum_freq,
                depth: sum_depth,
                tree: HuffTree::Node {
                    left: nodes.len() - 1,
                    right: nodes.len() - 2,
                },
            };

            if heap.len() == 1 {
                // last node goes at the end
                nodes.push(node);
                break;
            } else {
                heap[SMALLEST] = node;
                pqdownheap(&mut heap, 0);
            }
        }

        fn count_recursive(
            n: &[HuffTreeNode],
            index: usize,
            node_bit_len: &mut Vec<u8>,
            depth: u8,
        ) {
            match n[index].tree {
                HuffTree::Leaf(symbol) => node_bit_len[symbol] = depth,
                HuffTree::Node { left, right } => {
                    count_recursive(n, left, node_bit_len, depth + 1);
                    count_recursive(n, right, node_bit_len, depth + 1);
                }
            }
        }

        // assign the bit lengths for each symbol by walking down the tree
        // and counting the depth of each leaf node
        count_recursive(&nodes, nodes.len() - 1, &mut node_bit_len, 0);

        // enforce the maximum bit length by counting the number of symbols that
        // have a bit length greater than the maximum and then redistributing
        let mut bl_count = vec![0; max_bits + 1];
        let mut overflow = 0;
        for &bit_len in &node_bit_len {
            let mut new_len: usize = bit_len.into();

            if new_len > max_bits {
                new_len = max_bits;
                overflow += 1;
            }

            bl_count[new_len] += 1;
        }

        if overflow > 0 {
            // redistribute the bit lengths to remove the overflow
            while overflow > 0 {
                let mut bits = max_bits - 1;
                while bl_count[bits] == 0 {
                    bits -= 1;
                }

                bl_count[bits] -= 1;
                bl_count[bits + 1] += 2;
                bl_count[max_bits] -= 1;

                overflow -= 2;
            }

            // now reassign the bitlengths to the nodes (since we already have them in the right order)
            let mut bits = max_bits;
            for node in nodes.iter() {
                if let HuffTree::Leaf(idx) = node.tree {
                    while bl_count[bits] == 0 {
                        bits -= 1;
                    }

                    node_bit_len[idx] = bits as u8;
                    bl_count[bits] -= 1;
                }
            }
        }

        node_bit_len
    }

    #[cfg(test)]
    fn test_result(sym_freq: &[u16], max_bits: usize, expected: &[u8]) {
        let result = calc_bit_lengths(sym_freq, max_bits);
        assert_eq!(result[..], expected[..]);
    }

    #[test]
    fn sift_down_t() {
        let freq = [10, 9, 8, 7, 6, 5, 4];

        let mut heap: Vec<HuffTreeNode> = freq
            .iter()
            .map(|&x| HuffTreeNode {
                freq: x,
                depth: 0,
                tree: HuffTree::Leaf(0),
            })
            .collect();

        println!("{:?}", heap.iter().map(|x| x.freq).collect::<Vec<_>>());
        pqdownheap(&mut heap, 0);
        println!("{:?}", heap.iter().map(|x| x.freq).collect::<Vec<_>>());

        let mut n = heap.len() - 1;
        while n >= 1 {
            n -= 1;
            pqdownheap(&mut heap, n);
        }

        loop {
            println!("{}", heap[0].freq);

            if heap.len() == 1 {
                break;
            }
            heap[0] = heap.pop().unwrap();
            pqdownheap(&mut heap, 0);
        }
    }

    #[test]
    fn roundtrip_huffman_code_simple() {
        test_result(&[0, 1, 2, 4, 8, 16, 32], 7, &[0, 5, 5, 4, 3, 2, 1]);
    }

    // requires overflow treatment
    #[test]
    fn roundtrip_huffman_code_overflow() {
        test_result(
            &[
                1, 0, 1, 1, 5, 10, 9, 18, 29, 59, 91, 28, 11, 1, 2, 0, 12, 1, 0,
            ],
            7,
            &[7, 0, 7, 7, 6, 5, 5, 4, 3, 2, 2, 3, 5, 7, 7, 0, 5, 7],
        );
    }

    #[test]
    fn roundtrip_huffman_code_normal() {
        test_result(
            &[
                1, 0, 1, 0, 9, 9, 8, 19, 36, 70, 73, 34, 5, 6, 0, 0, 11, 1, 0,
            ],
            7,
            &[7, 0, 7, 0, 5, 5, 5, 4, 3, 2, 2, 3, 7, 5, 0, 0, 5, 7],
        );
    }
}
