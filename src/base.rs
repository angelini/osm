use std::ffi::OsStr;
use std::fmt;
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
