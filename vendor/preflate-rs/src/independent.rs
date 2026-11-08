/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the Apache License, Version 2.0. See LICENSE.txt in the project root for license information.
 *  This software incorporates material from third parties. See NOTICE.txt for details.
 *--------------------------------------------------------------------------------------------*/

//! Independently rebuildable chunks. Each chunk restarts the encoder model from the plain text
//! just before it (hash chains seeded from the window; pending matches already reset per block),
//! so chunks can be analyzed and recreated in parallel. The output is spliced at bit level.

use std::{
    io::Cursor,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use bitcode::{Decode, Encode};

use crate::{
    PreflateConfig, Result,
    cabac::vp8::{VP8Reader, VP8Writer},
    cabac_codec::{PredictionDecoderCabac, PredictionEncoderCabac},
    deflate::{
        deflate_reader::DeflateParser,
        deflate_token::{DeflateToken, DeflateTokenBlock, DeflateTokenBlockType},
        deflate_writer::DeflateWriter,
    },
    estimator::preflate_parameter_estimator::{
        TokenPredictorParameters, estimate_preflate_parameters,
    },
    preflate_error::{ExitCode, PreflateError},
    preflate_input::{PlainText, PreflateInput},
    statistical_codec::PredictionEncoder,
    stream_processor::{predict_blocks, recreate_blocks},
    token_predictor::TokenPredictor,
};

/// Version byte in front of the serialized chunks.
const INDEPENDENT_DATA_VERSION: u8 = 0xA0;

/// Window plus a margin, so matches at the full window distance still see their source.
const PREFIX_LEN: usize = 32768 + 1024;

#[derive(Encode, Decode)]
struct IndependentChunk {
    plain_len: u32,
    start_bit: u8,
    corrections: Vec<u8>,
}

#[derive(Encode, Decode)]
struct IndependentData {
    parameters: TokenPredictorParameters,
    chunks: Vec<IndependentChunk>,
}

fn block_plain_len(block: &DeflateTokenBlock) -> usize {
    match &block.block_type {
        DeflateTokenBlockType::Stored { uncompressed } => uncompressed.len(),
        DeflateTokenBlockType::Huffman { tokens, .. } => tokens
            .iter()
            .map(|t| match t {
                DeflateToken::Literal(_) => 1,
                DeflateToken::Reference(r) => r.len() as usize,
            })
            .sum(),
    }
}

/// The chunk's text with up to PREFIX_LEN bytes before it.
fn chunk_text(text: &[u8], start: usize, len: usize) -> (PlainText, u32) {
    let prefix = start.min(PREFIX_LEN);
    let base = start - prefix;
    (
        window_text(&text[base..start + len], base, prefix),
        prefix as u32,
    )
}

/// Plain text starting at offset `base` whose first `prefix` bytes are history. Positions keep
/// the original value modulo 4096, since some add policies depend on it.
fn window_text(window: &[u8], base: usize, prefix: usize) -> PlainText {
    let pos = (base % 4096 + prefix) as i32;
    PlainText::new_with_prefix(&window[..prefix], &window[prefix..], pos)
}

fn parallel<R: Send>(n: usize, threads: usize, f: impl Fn(usize) -> R + Sync) -> Vec<R> {
    let next = AtomicUsize::new(0);
    let out: Vec<Mutex<Option<R>>> = (0..n).map(|_| Mutex::new(None)).collect();
    std::thread::scope(|s| {
        for _ in 0..threads.clamp(1, n.max(1)) {
            s.spawn(|| {
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= n {
                        break;
                    }
                    *out[i].lock().unwrap() = Some(f(i));
                }
            });
        }
    });
    out.into_iter()
        .map(|m| m.into_inner().unwrap().unwrap())
        .collect()
}

/// Analyzes a whole deflate stream into chunks of about `chunk_plain` plain bytes.
/// Returns the reconstruction data, the deflate stream length and the plain text.
pub fn preflate_independent(
    compressed: &[u8],
    config: &PreflateConfig,
    chunk_plain: usize,
    threads: usize,
) -> Result<(Vec<u8>, usize, Vec<u8>)> {
    let mut parser = DeflateParser::new(config.plain_text_limit);
    let contents = parser.parse(compressed)?;
    if !parser.is_done() {
        return Err(PreflateError::new(
            ExitCode::PlainTextLimit,
            "stream exceeds plain text limit",
        ));
    }
    let plain = parser.detach_plain_text();
    let params = estimate_preflate_parameters(&contents, &plain)?;
    if params.max_chain > config.max_chain_length {
        return Err(PreflateError::new(
            ExitCode::NoCompressionCandidates,
            "max_chain too large",
        ));
    }

    // group whole blocks; the bit position of each group start comes from re-encoding the blocks
    let blocks = &contents.blocks;
    let mut writer = DeflateWriter::new();
    let mut groups = Vec::new(); // (first block, end block, plain start, plain len, start bit)
    let (mut first, mut start, mut start_bit, mut pos) = (0, 0, 0u8, 0);
    for (i, block) in blocks.iter().enumerate() {
        if i > first && pos - start >= chunk_plain {
            groups.push((first, i, start, pos - start, start_bit));
            (first, start, start_bit) = (i, pos, (writer.bit_position() % 8) as u8);
        }
        writer.encode_block(block)?;
        pos += block_plain_len(block);
    }
    groups.push((first, blocks.len(), start, pos - start, start_bit));
    writer.flush();
    if writer.detach_output()[..] != compressed[..contents.compressed_size] {
        return Err(PreflateError::new(
            ExitCode::RoundtripMismatch,
            "re-encoded blocks differ",
        ));
    }

    let text = plain.text();
    let chunks = parallel(groups.len(), threads, |g| -> Result<IndependentChunk> {
        let (first, end, start, len, start_bit) = groups[g];
        let (chunk, prefix) = chunk_text(text, start, len);
        let mut input = PreflateInput::new(&chunk);
        let mut predictor = TokenPredictor::new(&params);
        if prefix > 0 {
            predictor.seed_dictionary(&input, prefix);
        }
        let mut corrections = Vec::new();
        {
            let mut encoder =
                PredictionEncoderCabac::new(VP8Writer::new(&mut corrections).unwrap());
            predict_blocks(
                &blocks[first..end],
                &mut predictor,
                &mut encoder,
                &mut input,
            )?;
            encoder.finish();
        }
        Ok(IndependentChunk {
            plain_len: len as u32,
            start_bit,
            corrections,
        })
    })
    .into_iter()
    .collect::<Result<Vec<_>>>()?;

    let mut data = vec![INDEPENDENT_DATA_VERSION];
    data.extend_from_slice(&bitcode::encode(&IndependentData {
        parameters: params,
        chunks,
    }));
    Ok((data, contents.compressed_size, plain.text().to_vec()))
}

/// Recreates the exact deflate stream from its plain text and `preflate_independent` data.
pub fn recreate_independent(plain_text: &[u8], data: &[u8], threads: usize) -> Result<Vec<u8>> {
    let recipe = IndependentRecipe::parse(data)?;
    let parts = parallel(recipe.groups(), threads, |g| {
        let (base, _, end) = recipe.plain_range(g);
        let window = plain_text.get(base..end).ok_or_else(|| {
            PreflateError::new(
                ExitCode::InvalidParameter,
                "plain text shorter than the recipe",
            )
        })?;
        recipe.recreate_group(g, window)
    });
    let mut splicer = Splicer::default();
    let mut out = Vec::new();
    for part in parts {
        out.extend(splicer.push(part?));
    }
    out.extend(splicer.finish());
    Ok(out)
}

/// One rebuilt group: whole bytes, then a final byte holding `bits` bits.
pub struct RebuiltGroup {
    bytes: Vec<u8>,
    last: u8,
    bits: u32,
}

/// `preflate_independent` data parsed once, so groups can be rebuilt one at a time, in any
/// order, and streamed.
pub struct IndependentRecipe {
    data: IndependentData,
    starts: Vec<usize>,
}

impl IndependentRecipe {
    pub fn parse(data: &[u8]) -> Result<Self> {
        let data: IndependentData = match data.split_first() {
            Some((&INDEPENDENT_DATA_VERSION, payload)) => {
                bitcode::decode(payload).map_err(|e| {
                    PreflateError::new(
                        ExitCode::InvalidCompressedWrapper,
                        format!("{e:?}").as_str(),
                    )
                })?
            }
            _ => {
                return Err(PreflateError::new(
                    ExitCode::InvalidCompressedWrapper,
                    "not independent chunk data",
                ));
            }
        };
        let starts = data
            .chunks
            .iter()
            .scan(0usize, |acc, c| {
                let s = *acc;
                *acc += c.plain_len as usize;
                Some(s)
            })
            .collect();
        Ok(Self { data, starts })
    }

    pub fn groups(&self) -> usize {
        self.data.chunks.len()
    }

    /// The encoder model every group replays.
    pub fn parameters(&self) -> &TokenPredictorParameters {
        &self.data.parameters
    }

    /// Plain text group `g` needs, as offsets: (history start, group start, group end).
    pub fn plain_range(&self, g: usize) -> (usize, usize, usize) {
        let start = self.starts[g];
        let end = start + self.data.chunks[g].plain_len as usize;
        (start - start.min(PREFIX_LEN), start, end)
    }

    /// Rebuilds group `g` from the plain text between its history start and its end.
    pub fn recreate_group(&self, g: usize, window: &[u8]) -> Result<RebuiltGroup> {
        let (base, start, end) = self.plain_range(g);
        if window.len() != end - base {
            return Err(PreflateError::new(
                ExitCode::InvalidParameter,
                "window does not match the group",
            ));
        }
        let c = &self.data.chunks[g];
        let prefix = start - base;
        let text = window_text(window, base, prefix);
        let mut input = PreflateInput::new(&text);
        let mut predictor = TokenPredictor::new(&self.data.parameters);
        if prefix > 0 {
            predictor.seed_dictionary(&input, prefix as u32);
        }
        let mut decoder =
            PredictionDecoderCabac::new(VP8Reader::new(Cursor::new(&c.corrections[..])).unwrap());
        let mut writer = DeflateWriter::new_at_bit_offset(u32::from(c.start_bit));
        recreate_blocks(&mut predictor, &mut decoder, &mut writer, &mut input)?;
        let (bytes, last, bits) = writer.detach_with_partial();
        Ok(RebuiltGroup { bytes, last, bits })
    }
}

/// Joins rebuilt groups, in order, into the deflate stream. Each group's first byte starts with
/// the previous group's trailing bits as zeros.
#[derive(Default)]
pub struct Splicer {
    carry: Option<u8>,
}

impl Splicer {
    /// Continues a stream at a group whose predecessor left `carry` (see `carry`).
    pub fn resume(carry: Option<u8>) -> Self {
        Self { carry }
    }

    /// Trailing bits held for the next group, to record where a stream can resume.
    pub fn carry(&self) -> Option<u8> {
        self.carry
    }

    /// The next part of the stream; the group's trailing bits are held for the next group.
    pub fn push(&mut self, group: RebuiltGroup) -> Vec<u8> {
        let RebuiltGroup {
            mut bytes,
            mut last,
            bits,
        } = group;
        if let Some(prev) = self.carry.take() {
            match bytes.first_mut() {
                Some(b) => *b |= prev,
                None => last |= prev,
            }
        }
        self.carry = (bits > 0).then_some(last);
        bytes
    }

    /// The final partial byte, if any.
    pub fn finish(self) -> Option<u8> {
        self.carry
    }
}

#[test]
fn independent_chunks_roundtrip() {
    use crate::utils::read_file;

    let mut names = vec![
        "compressed_go_level6_one_distance.deflate".to_string(),
        "compressed_go_level6_no_match.deflate".to_string(),
    ];
    for lib in ["zlib", "libdeflate", "flate2"] {
        names.extend((0..10).map(|l| format!("compressed_{lib}_level{l}.deflate")));
    }
    names.extend((1..5).map(|l| format!("compressed_zlibng_level{l}.deflate")));
    names.push("compressed_minizoxide_level1.deflate".into());

    for name in &names {
        let data = read_file(name);
        for chunk_plain in [1 << 10, 1 << 14, usize::MAX] {
            let (recipe, size, plain) =
                preflate_independent(&data, &PreflateConfig::default(), chunk_plain, 4).unwrap();
            let rebuilt = recreate_independent(&plain, &recipe, 4).unwrap();
            assert!(
                rebuilt[..] == data[..size],
                "{name}, chunks of {chunk_plain}"
            );

            // groups rebuild in any order and splice in order
            let r = IndependentRecipe::parse(&recipe).unwrap();
            let mut parts: Vec<_> = (0..r.groups())
                .rev()
                .map(|g| {
                    let (base, _, end) = r.plain_range(g);
                    (g, r.recreate_group(g, &plain[base..end]).unwrap())
                })
                .collect();
            parts.sort_by_key(|(g, _)| *g);
            let mut splicer = Splicer::default();
            let mut streamed = Vec::new();
            for (_, part) in parts {
                streamed.extend(splicer.push(part));
            }
            streamed.extend(splicer.finish());
            assert!(
                streamed[..] == data[..size],
                "{name}, streamed, chunks of {chunk_plain}"
            );

            // resuming at any group with the recorded carry continues the same stream
            let mut splicer = Splicer::default();
            let mut starts = Vec::new();
            let mut pos = 0;
            for g in 0..r.groups() {
                starts.push((pos, splicer.carry()));
                let (base, _, end) = r.plain_range(g);
                pos += splicer
                    .push(r.recreate_group(g, &plain[base..end]).unwrap())
                    .len();
            }
            for (g, &(at, carry)) in starts.iter().enumerate() {
                let mut splicer = Splicer::resume(carry);
                let mut tail = Vec::new();
                for h in g..r.groups() {
                    let (base, _, end) = r.plain_range(h);
                    tail.extend(splicer.push(r.recreate_group(h, &plain[base..end]).unwrap()));
                }
                tail.extend(splicer.finish());
                assert!(
                    tail[..] == data[at..size],
                    "{name}, resumed at group {g}, chunks of {chunk_plain}"
                );
            }
        }
    }
}
