use std::ffi::OsStr;
use std::fmt;
use std::ops::Add;
use std::path::PathBuf;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Format {
    Csv,
    Parquet,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Compression {
    None,
    Gzip,
    Snappy,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Protocol {
    File,
    S3,
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let str = match self {
            Protocol::File => "file",
            Protocol::S3 => "s3",
        };
        write!(f, "{}", str)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ObjectKey(String);

impl ObjectKey {
    pub fn from_os_str(s: &OsStr) -> Self {
        ObjectKey(s.to_string_lossy().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn extension(&self) -> Option<&str> {
        self.0.split('.').collect::<Vec<&str>>().get(1).copied()
    }
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub trait ToStdPath {
    fn std_path(&self) -> PathBuf;
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Partition {
    values: Vec<(String, String)>,
}

impl Partition {
    pub fn new<S: Into<String>>(key: S, value: S) -> Partition {
        Partition {
            values: vec![(key.into(), value.into())],
        }
    }

    pub fn push(&self, key: String, value: String) -> Partition {
        let mut values = self.values.clone();
        values.push((key, value));
        Partition { values }
    }
}

impl ToStdPath for Partition {
    fn std_path(&self) -> PathBuf {
        let mut buf = PathBuf::new();
        for (key, value) in &self.values {
            buf.push(format!("{}={}", key, value));
        }
        buf
    }
}

impl fmt::Display for Partition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for (key, value) in &self.values {
            write!(f, "{}={}", key, value)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Bucket {
    pub protocol: Protocol,
    pub name: String,
}

impl Bucket {
    pub fn new(protocol: Protocol, name: String) -> Self {
        Bucket { protocol, name }
    }
}

impl fmt::Display for Bucket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}://{}", self.protocol, self.name)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Output(String);

impl fmt::Display for Output {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct Bytes(usize);

impl Bytes {
    const KIB: usize = 1024;
    const MIB: usize = Self::KIB * 1024;

    const KIB_THRESHOLD: usize = 10 * Self::KIB;
    const MIB_THRESHOLD: usize = 10 * Self::MIB;

    pub fn new(size: usize) -> Self {
        Self(size)
    }
}

impl fmt::Display for Bytes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.0 {
            size if size > Self::MIB_THRESHOLD => {
                write!(f, "{:.2} MiB", size as f64 / Self::MIB as f64)
            }
            size if size > Self::KIB_THRESHOLD => {
                write!(f, "{:.2} KiB", size as f64 / Self::KIB as f64)
            }
            size => write!(f, "{} B", size),
        }
    }
}

impl Add for Bytes {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self::new(self.0 + other.0)
    }
}
