use core::fmt;
use std::error::Error;
use crate::chained_error::Source::{Own, Empty};
use crate::chained_error::Source::Ref;
use std::fmt::{Formatter, Display};
use std::any::Any;


#[derive(Debug)]
pub struct Chained {
    description: String,
    source: Source,
}

enum Source {
    Empty,
    Own(Box<dyn Error + Send + Sync>),
    Ref(Box<dyn Any + Send + Sync>, Box<dyn Fn(&dyn Any) -> Option<&(dyn Error + 'static)> + Send + Sync>)
}

impl fmt::Debug for Source {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Empty => write!(f, "Source {{ Empty }}"),
            Own(e) => write!(f, "Source {{ Own({}) }}", e),
            Ref(_, _) => write!(f, "Source {{ Ref() }}")
        }
    }
}

impl Chained {
    pub fn without_source(description: impl AsRef<str>) -> Chained {
        Chained {
            description: description.as_ref().to_string(),
            source: Empty
        }
    }

    pub fn new<E>(description: impl AsRef<str>, source: E) -> Chained
        where E: Into<Box<dyn Error + Send + Sync>>
    {
        Chained {
            description: description.as_ref().to_string(),
            source: Own(source.into())
        }
    }

    pub fn optional<E>(description: impl AsRef<str>, source: Option<E>) -> Chained
        where E: Into<Box<dyn Error + Send + Sync>>
    {
        Chained {
            description: description.as_ref().to_string(),
            source: match source {
                Some(e) => Own(e.into()),
                None => Empty
            }
        }
    }

    pub fn result<T, E>(description: impl AsRef<str>, source: Result<T, E>) -> Chained
        where E: Into<Box<dyn Error + Send + Sync>>
    {
        Chained::optional(description, source.err())
    }

    pub fn monad(description: impl AsRef<str>, value: Box<dyn Any + Send + Sync>,
                    func: Box<dyn Fn(&dyn Any) -> Option<&(dyn Error + 'static)> + Send + Sync>) -> Chained {
        Chained {
            description: description.as_ref().to_string(),
            source: Ref(value, func),
        }
    }
}


impl fmt::Display for Chained {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        
        fn write_some(f: &mut fmt::Formatter<'_>, description: &str, e: &dyn Error) -> fmt::Result {
            write!(f, "{}\nCaused by: {}", description, e)
        }

        fn write_none(f: &mut fmt::Formatter<'_>, description: &str) -> fmt::Result {
            Display::fmt(description, f)
        }

        match &self.source {
            Empty => write_none(f, &self.description),
            Own(e) => write_some(f, &self.description, &**e),
            Ref(v, func) => match func(&**v) {
                Some(e) => write_some(f, &self.description, e),
                None => write_none(f, &self.description),
            }
        }
    }
}

impl Error for Chained {
    fn description(&self) -> &str {
        &self.description
    }

    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.source {
            Empty => None,
            Own(e) => Some(&**e),
            Ref(v, func) => func(&**v)
        }
    }
}


#[cfg(test)]
mod test {
    use crate::chained_error::Chained;
    use std::error::Error;
    use std::sync::Arc;
    use std::error;

    #[test]
    fn without_source() {
        let c = Chained::without_source("error");
        assert_eq!("error", &c.to_string());
        assert!(c.source().is_none());
    }

    #[test]
    fn new() {
        let c = Chained::new("error", "parent");
        assert_eq!("error\nCaused by: parent", &c.to_string());
        assert_eq!("parent", c.source().unwrap().to_string());
    }

    #[test]
    fn optional_some() {
        let c = Chained::optional("error", Some("parent"));
        assert_eq!("error\nCaused by: parent", &c.to_string());
        assert_eq!("parent", c.source().unwrap().to_string());
    }

    #[test]
    fn optional_none() {
        let c = Chained::optional("error", Option::<String>::None);
        assert_eq!("error", &c.to_string());
        assert!(c.source().is_none());
    }

    #[test]
    fn result_err() {
        let c = Chained::result("error", Result::<(), &str>::Err("parent"));
        assert_eq!("error\nCaused by: parent", &c.to_string());
        assert_eq!("parent", c.source().unwrap().to_string());
    }

    #[test]
    fn result_ok() {
        let c = Chained::result("error", Result::<(), String>::Ok(()));
        assert_eq!("error", &c.to_string());
        assert!(c.source().is_none());
    }

    #[test]
    fn monad() {
        let c = Chained::monad("error",
                               Box::new(Arc::<dyn Error + Send + Sync>::from(Box::<dyn Error + Send + Sync>::from("parent"))),
                               Box::new(|any|
                                   any.downcast_ref::<Arc<dyn Error + Send + Sync>>()
                                       .map(|arc| &**arc)
                                       .map(|e| e as &(dyn Error))));
        assert_eq!("error\nCaused by: parent", &c.to_string());
        assert_eq!("parent", c.source().unwrap().to_string());
    }
}