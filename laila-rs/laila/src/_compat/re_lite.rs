//! A small backtracking regex engine: the subset of Python's `re` that the
//! package and its suites use (`GLOBAL_ID_REGEX_PATTERN`, the HEVC
//! emulation-prevention scanner, `assertRegex`) -- literals and escapes
//! (`\d \w \s \D \W \S \xHH \t \n \r`), `.`, `^`, `$`, classes
//! (`[a-z0-9_]`, `[^,@]`), groups (`(..)`, `(?:..)`, `(?P<name>..)`),
//! lookahead (`(?=..)`), the quantifiers `* + ? {n} {n,} {n,m}` (greedy
//! and lazy), and alternation. Matching is byte-wise (the patterns are
//! ASCII; multi-byte UTF-8 text matches literally), `no_std` + `alloc`.

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;

#[derive(Clone, Debug)]
enum Node {
    /// A single byte.
    Byte(u8),
    /// `.` (anything but a newline, as Python without DOTALL).
    Any,
    /// `[...]`: 256-entry membership table.
    Class(Box<[bool; 256]>),
    /// `^`
    Start,
    /// `$`
    End,
    /// A capturing or non-capturing group of alternatives.
    Group {
        index: Option<usize>,
        alts: Vec<Vec<Node>>,
    },
    /// `(?=..)`
    Lookahead(Vec<Vec<Node>>),
    /// `x{min,max}` (`max == None` is unbounded).
    Repeat {
        node: Box<Node>,
        min: usize,
        max: Option<usize>,
        greedy: bool,
    },
}

/// A compiled pattern (`re.compile(p)`).
#[derive(Clone, Debug)]
pub struct Regex {
    pub pattern: String,
    alts: Vec<Vec<Node>>,
    /// `(?P<name>..)` names by group index (1-based).
    names: Vec<(String, usize)>,
    groups: usize,
}

/// `re.Match`: the span of the whole match and of every group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Match {
    pub start: usize,
    pub end: usize,
    /// Group spans, index 0 = whole match.
    pub groups: Vec<Option<(usize, usize)>>,
    names: Vec<(String, usize)>,
}

impl Match {
    /// `m.group(i)`.
    pub fn group<'t>(&self, text: &'t str, i: usize) -> Option<&'t str> {
        let (s, e) = (*self.groups.get(i)?)?;
        text.get(s..e)
    }
    /// `m.group("name")`.
    pub fn named<'t>(&self, text: &'t str, name: &str) -> Option<&'t str> {
        let (_, i) = self.names.iter().find(|(n, _)| n == name)?;
        self.group(text, *i)
    }
    /// The `(name, group index)` table of the pattern's named groups.
    pub fn names_with_index(&self) -> Vec<(String, usize)> {
        self.names.clone()
    }
    /// `m.groupdict()` -- `(name, value)` pairs.
    pub fn groupdict<'t>(&self, text: &'t str) -> Vec<(&str, Option<&'t str>)> {
        self.names
            .iter()
            .map(|(n, i)| (n.as_str(), self.group(text, *i)))
            .collect()
    }
}

struct Parser<'p> {
    p: &'p [u8],
    i: usize,
    groups: usize,
    names: Vec<(String, usize)>,
}

impl<'p> Parser<'p> {
    fn peek(&self) -> Option<u8> {
        self.p.get(self.i).copied()
    }
    fn bump(&mut self) -> Option<u8> {
        let c = self.peek()?;
        self.i += 1;
        Some(c)
    }
    fn alts(&mut self) -> Result<Vec<Vec<Node>>, String> {
        let mut alts = alloc::vec![Vec::new()];
        loop {
            match self.peek() {
                None => break,
                Some(b')') => break,
                Some(b'|') => {
                    self.i += 1;
                    alts.push(Vec::new());
                }
                Some(_) => {
                    let atom = self.atom()?;
                    let atom = self.quantified(atom)?;
                    alts.last_mut().unwrap().push(atom);
                }
            }
        }
        Ok(alts)
    }
    fn quantified(&mut self, node: Node) -> Result<Node, String> {
        let (min, max) = match self.peek() {
            Some(b'*') => {
                self.i += 1;
                (0, None)
            }
            Some(b'+') => {
                self.i += 1;
                (1, None)
            }
            Some(b'?') => {
                self.i += 1;
                (0, Some(1))
            }
            Some(b'{') => {
                let save = self.i;
                self.i += 1;
                let mut lo = String::new();
                while let Some(c) = self.peek().filter(u8::is_ascii_digit) {
                    lo.push(c as char);
                    self.i += 1;
                }
                let (min, max) = match self.peek() {
                    Some(b'}') if !lo.is_empty() => {
                        let n: usize = lo.parse().unwrap();
                        (n, Some(n))
                    }
                    Some(b',') => {
                        self.i += 1;
                        let mut hi = String::new();
                        while let Some(c) = self.peek().filter(u8::is_ascii_digit) {
                            hi.push(c as char);
                            self.i += 1;
                        }
                        if self.peek() != Some(b'}') {
                            self.i = save;
                            return Ok(node);
                        }
                        let min: usize = if lo.is_empty() {
                            0
                        } else {
                            lo.parse().unwrap()
                        };
                        let max = if hi.is_empty() {
                            None
                        } else {
                            Some(hi.parse::<usize>().unwrap())
                        };
                        (min, max)
                    }
                    _ => {
                        // a literal `{`
                        self.i = save;
                        return Ok(node);
                    }
                };
                self.i += 1; // `}`
                (min, max)
            }
            _ => return Ok(node),
        };
        let greedy = if self.peek() == Some(b'?') {
            self.i += 1;
            false
        } else {
            true
        };
        Ok(Node::Repeat {
            node: Box::new(node),
            min,
            max,
            greedy,
        })
    }
    fn escape_class(c: u8, table: &mut [bool; 256]) -> bool {
        let set = |table: &mut [bool; 256], f: &dyn Fn(u8) -> bool, negate: bool| {
            for b in 0..=255u8 {
                if f(b) != negate {
                    table[b as usize] = true;
                }
            }
        };
        match c {
            b'd' => set(table, &|b| b.is_ascii_digit(), false),
            b'D' => set(table, &|b| b.is_ascii_digit(), true),
            b'w' => set(table, &|b| b.is_ascii_alphanumeric() || b == b'_', false),
            b'W' => set(table, &|b| b.is_ascii_alphanumeric() || b == b'_', true),
            b's' => set(
                table,
                &|b| matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c),
                false,
            ),
            b'S' => set(
                table,
                &|b| matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c),
                true,
            ),
            _ => return false,
        }
        true
    }
    fn escape_byte(&mut self, c: u8) -> Result<u8, String> {
        Ok(match c {
            b'n' => b'\n',
            b't' => b'\t',
            b'r' => b'\r',
            b'0' => 0,
            b'x' => {
                let h = self
                    .p
                    .get(self.i..self.i + 2)
                    .and_then(|h| core::str::from_utf8(h).ok())
                    .and_then(|h| u8::from_str_radix(h, 16).ok())
                    .ok_or_else(|| String::from("incomplete escape \\x"))?;
                self.i += 2;
                h
            }
            other => other,
        })
    }
    fn atom(&mut self) -> Result<Node, String> {
        let c = self
            .bump()
            .ok_or_else(|| String::from("unexpected end of pattern"))?;
        Ok(match c {
            b'.' => Node::Any,
            b'^' => Node::Start,
            b'$' => Node::End,
            b'\\' => {
                let e = self
                    .bump()
                    .ok_or_else(|| String::from("bad escape (end of pattern)"))?;
                let mut table = [false; 256];
                if Self::escape_class(e, &mut table) {
                    Node::Class(Box::new(table))
                } else {
                    Node::Byte(self.escape_byte(e)?)
                }
            }
            b'[' => {
                let mut table = [false; 256];
                let negate = if self.peek() == Some(b'^') {
                    self.i += 1;
                    true
                } else {
                    false
                };
                let mut first = true;
                loop {
                    let c = self
                        .bump()
                        .ok_or_else(|| String::from("unterminated character set"))?;
                    if c == b']' && !first {
                        break;
                    }
                    first = false;
                    let lo = if c == b'\\' {
                        let e = self.bump().ok_or_else(|| String::from("bad escape"))?;
                        if Self::escape_class(e, &mut table) {
                            continue;
                        }
                        self.escape_byte(e)?
                    } else {
                        c
                    };
                    if self.peek() == Some(b'-')
                        && self.p.get(self.i + 1).is_some_and(|&n| n != b']')
                    {
                        self.i += 1;
                        let h = self.bump().unwrap();
                        let hi = if h == b'\\' {
                            let e = self.bump().ok_or_else(|| String::from("bad escape"))?;
                            self.escape_byte(e)?
                        } else {
                            h
                        };
                        if hi < lo {
                            return Err(alloc::format!(
                                "bad character range {}-{}",
                                lo as char,
                                hi as char
                            ));
                        }
                        for b in lo..=hi {
                            table[b as usize] = true;
                        }
                    } else {
                        table[lo as usize] = true;
                    }
                }
                if negate {
                    for b in table.iter_mut() {
                        *b = !*b;
                    }
                }
                Node::Class(Box::new(table))
            }
            b'(' => {
                let mut index = None;
                let mut lookahead = false;
                if self.peek() == Some(b'?') {
                    self.i += 1;
                    match self.bump() {
                        Some(b':') => {}
                        Some(b'=') => lookahead = true,
                        Some(b'P') if self.peek() == Some(b'<') => {
                            self.i += 1;
                            let mut name = String::new();
                            loop {
                                match self.bump() {
                                    Some(b'>') => break,
                                    Some(c) => name.push(c as char),
                                    None => {
                                        return Err(String::from("missing >, unterminated name"))
                                    }
                                }
                            }
                            self.groups += 1;
                            index = Some(self.groups);
                            self.names.push((name, self.groups));
                        }
                        _ => return Err(String::from("unknown extension")),
                    }
                } else {
                    self.groups += 1;
                    index = Some(self.groups);
                }
                let alts = self.alts()?;
                if self.bump() != Some(b')') {
                    return Err(String::from("missing ), unterminated subpattern"));
                }
                if lookahead {
                    Node::Lookahead(alts)
                } else {
                    Node::Group { index, alts }
                }
            }
            b')' => return Err(String::from("unbalanced parenthesis")),
            b'*' | b'+' | b'?' => return Err(String::from("nothing to repeat")),
            other => Node::Byte(other),
        })
    }
}

impl Regex {
    /// `re.compile(pattern)`.
    pub fn new(pattern: &str) -> Result<Regex, String> {
        let mut p = Parser {
            p: pattern.as_bytes(),
            i: 0,
            groups: 0,
            names: Vec::new(),
        };
        let alts = p.alts()?;
        if p.i != pattern.len() {
            return Err(String::from("unbalanced parenthesis"));
        }
        Ok(Regex {
            pattern: String::from(pattern),
            alts,
            names: p.names,
            groups: p.groups,
        })
    }
    /// `pattern.match(text)`: a match anchored at the start.
    pub fn match_at_start(&self, text: &[u8]) -> Option<Match> {
        self.try_at(text, 0)
    }
    /// `pattern.fullmatch(text)`.
    pub fn fullmatch(&self, text: &[u8]) -> Option<Match> {
        let mut caps = alloc::vec![None; self.groups + 1];
        let end = match_alts(&self.alts, text, 0, &mut caps, &mut |e, _| e == text.len())?;
        caps[0] = Some((0, end));
        Some(self.finish(caps))
    }
    /// `pattern.search(text)`: the first match anywhere.
    pub fn search(&self, text: &[u8]) -> Option<Match> {
        (0..=text.len()).find_map(|i| self.try_at(text, i))
    }
    /// `pattern.finditer(text)`: every non-overlapping match.
    pub fn find_all(&self, text: &[u8]) -> Vec<Match> {
        let mut out = Vec::new();
        let mut i = 0;
        while i <= text.len() {
            match self.try_at(text, i) {
                Some(m) => {
                    i = if m.end == m.start { m.end + 1 } else { m.end };
                    out.push(m);
                }
                None => i += 1,
            }
        }
        out
    }
    fn try_at(&self, text: &[u8], start: usize) -> Option<Match> {
        let mut caps = alloc::vec![None; self.groups + 1];
        let end = match_alts(&self.alts, text, start, &mut caps, &mut |_, _| true)?;
        caps[0] = Some((start, end));
        Some(self.finish(caps))
    }
    fn finish(&self, caps: Vec<Option<(usize, usize)>>) -> Match {
        let (start, end) = caps[0].unwrap();
        Match {
            start,
            end,
            groups: caps,
            names: self.names.clone(),
        }
    }
}

type Caps = Vec<Option<(usize, usize)>>;

/// Match one alternative list at `pos`; `k` is the continuation that must
/// accept the end position (backtracking happens by `k` returning false).
fn match_alts(
    alts: &[Vec<Node>],
    text: &[u8],
    pos: usize,
    caps: &mut Caps,
    k: &mut dyn FnMut(usize, &mut Caps) -> bool,
) -> Option<usize> {
    for alt in alts {
        let saved = caps.clone();
        if let Some(e) = match_seq(alt, text, pos, caps, k) {
            return Some(e);
        }
        *caps = saved;
    }
    None
}

fn match_seq(
    seq: &[Node],
    text: &[u8],
    pos: usize,
    caps: &mut Caps,
    k: &mut dyn FnMut(usize, &mut Caps) -> bool,
) -> Option<usize> {
    let Some((first, rest)) = seq.split_first() else {
        return if k(pos, caps) { Some(pos) } else { None };
    };
    match first {
        Node::Byte(b) => {
            if text.get(pos) == Some(b) {
                match_seq(rest, text, pos + 1, caps, k)
            } else {
                None
            }
        }
        Node::Any => {
            if text.get(pos).is_some_and(|&c| c != b'\n') {
                match_seq(rest, text, pos + 1, caps, k)
            } else {
                None
            }
        }
        Node::Class(t) => {
            if text.get(pos).is_some_and(|&c| t[c as usize]) {
                match_seq(rest, text, pos + 1, caps, k)
            } else {
                None
            }
        }
        Node::Start => {
            if pos == 0 {
                match_seq(rest, text, pos, caps, k)
            } else {
                None
            }
        }
        Node::End => {
            // Python's `$`: the end, or just before a trailing newline.
            if pos == text.len() || (pos + 1 == text.len() && text[pos] == b'\n') {
                match_seq(rest, text, pos, caps, k)
            } else {
                None
            }
        }
        Node::Group { index, alts } => {
            let index = *index;
            let mut out = None;
            let r = match_alts(alts, text, pos, caps, &mut |e, caps| {
                let saved = index.map(|i| caps[i]);
                if let Some(i) = index {
                    caps[i] = Some((pos, e));
                }
                match match_seq(rest, text, e, caps, k) {
                    Some(fin) => {
                        out = Some(fin);
                        true
                    }
                    None => {
                        if let (Some(i), Some(s)) = (index, saved) {
                            caps[i] = s;
                        }
                        false
                    }
                }
            });
            r.and(out)
        }
        Node::Lookahead(alts) => {
            let saved = caps.clone();
            let ok = match_alts(alts, text, pos, caps, &mut |_, _| true).is_some();
            *caps = saved;
            if ok {
                match_seq(rest, text, pos, caps, k)
            } else {
                None
            }
        }
        Node::Repeat {
            node,
            min,
            max,
            greedy,
        } => match_repeat(node, *min, *max, *greedy, rest, text, pos, 0, caps, k),
    }
}

#[allow(clippy::too_many_arguments)]
fn match_repeat(
    node: &Node,
    min: usize,
    max: Option<usize>,
    greedy: bool,
    rest: &[Node],
    text: &[u8],
    pos: usize,
    count: usize,
    caps: &mut Caps,
    k: &mut dyn FnMut(usize, &mut Caps) -> bool,
) -> Option<usize> {
    let can_more = max.is_none_or(|m| count < m);
    let try_more =
        |caps: &mut Caps, k: &mut dyn FnMut(usize, &mut Caps) -> bool| -> Option<usize> {
            if !can_more {
                return None;
            }
            let one = core::slice::from_ref(node);
            let mut out = None;
            let r = match_seq(one, text, pos, caps, &mut |e, caps| {
                // An empty iteration cannot make progress: stop repeating.
                if e == pos {
                    return false;
                }
                match match_repeat(node, min, max, greedy, rest, text, e, count + 1, caps, k) {
                    Some(fin) => {
                        out = Some(fin);
                        true
                    }
                    None => false,
                }
            });
            r.and(out)
        };
    let try_stop =
        |caps: &mut Caps, k: &mut dyn FnMut(usize, &mut Caps) -> bool| -> Option<usize> {
            if count < min {
                return None;
            }
            match_seq(rest, text, pos, caps, k)
        };
    if greedy {
        let saved = caps.clone();
        if let Some(e) = try_more(caps, k) {
            return Some(e);
        }
        *caps = saved;
        try_stop(caps, k)
    } else {
        let saved = caps.clone();
        if let Some(e) = try_stop(caps, k) {
            return Some(e);
        }
        *caps = saved;
        try_more(caps, k)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(p: &str, t: &str) -> Option<(usize, usize)> {
        Regex::new(p)
            .unwrap()
            .search(t.as_bytes())
            .map(|m| (m.start, m.end))
    }

    #[test]
    fn basics() {
        assert_eq!(s("abc", "xxabcxx"), Some((2, 5)));
        assert_eq!(s("a.c", "abc"), Some((0, 3)));
        assert_eq!(s("^a", "ba"), None);
        assert_eq!(s("a$", "ba"), Some((1, 2)));
        assert_eq!(s("a+", "caaab"), Some((1, 4)));
        assert_eq!(s("a+?", "caaab"), Some((1, 2)));
        assert_eq!(s("a{2,3}", "caaaab"), Some((1, 4)));
        assert_eq!(s("[0-9]+", "ab123c"), Some((2, 5)));
        assert_eq!(s("[^,@]*", ",x"), Some((0, 0)));
        assert_eq!(s("(ab|cd)+", "xcdabz"), Some((1, 5)));
        assert_eq!(s(r"\d\w\s", "a1b c"), Some((1, 4)));
        assert_eq!(
            s(r"\x00\x00(?=[\x00-\x03])", "\x01\x00\x00\x02"),
            Some((1, 3))
        );
    }

    #[test]
    fn global_id_groups() {
        let r =
            Regex::new(crate::basics::definitions::identifiable_object::GLOBAL_ID_REGEX_PATTERN)
                .unwrap();
        let t = "LAILA:ENTRY:12345678-1234-1234-1234-123456789abc@evolution=2,nickname=x";
        let m = r.fullmatch(t.as_bytes()).unwrap();
        assert_eq!(m.named(t, "scopes"), Some("LAILA:ENTRY:"));
        assert_eq!(
            m.named(t, "uuid"),
            Some("12345678-1234-1234-1234-123456789abc")
        );
        assert_eq!(m.named(t, "attributes"), Some("evolution=2,nickname=x"));
        assert!(r
            .fullmatch(b"LAILA:12345678-1234-1234-1234-123456789abc@")
            .is_none());
        assert!(r
            .fullmatch(b"12345678-1234-1234-1234-123456789abc")
            .is_none());
    }
}
