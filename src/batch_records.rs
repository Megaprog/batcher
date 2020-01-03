pub trait RecordsBuilder<T, R>: Clone + Send + 'static {
    fn add(&mut self, record: T);
    fn len(&self) -> u32;
    fn size(&self) -> usize;
    fn build(self) -> R;
}

pub trait RecordsBuilderFactory<T, R, Builder: RecordsBuilder<T, R>>: Clone + Send + 'static {
    fn create_builder(&self) -> Builder;
}

impl<T, R, Builder> RecordsBuilderFactory<T, R, Builder> for fn() -> Builder
    where Builder: RecordsBuilder<T, R>
{
    fn create_builder(&self) -> Builder {
        self()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonArrayRecordsBuilder {
    json_array: String,
    len: u32,
    size: usize,
}

impl JsonArrayRecordsBuilder {
    pub const fn new() -> JsonArrayRecordsBuilder {
        JsonArrayRecordsBuilder {
            json_array: String::new(),
            len: 0,
            size: 2,
        }
    }

    pub fn with_capacity(capacity: usize) -> JsonArrayRecordsBuilder {
        JsonArrayRecordsBuilder {
            json_array: String::with_capacity(capacity),
            len: 0,
            size: 2,
        }
    }
}

impl RecordsBuilder<&str, String> for JsonArrayRecordsBuilder {
    fn add(&mut self, record: &str) {
        if self.len == 0 {
            self.json_array += "[";
            self.size -= 1;
        } else {
            self.json_array += ",";
        }

        self.json_array += record;
        self.len += 1;
        self.size += record.len() + 1;
    }

    fn len(&self) -> u32 {
        self.len
    }

    fn size(&self) -> usize {
        self.size
    }

    fn build(self) -> String {
        if self.len == 0 {
            "[]".to_string()
        } else {
            self.json_array + "]"
        }
    }
}

#[derive(Clone, Debug)]
pub struct JsonArrayRecordsBuilderFactory;

impl RecordsBuilderFactory<&str, String, JsonArrayRecordsBuilder> for JsonArrayRecordsBuilderFactory {
    fn create_builder(&self) -> JsonArrayRecordsBuilder {
        JsonArrayRecordsBuilder::new()
    }
}

pub const RECORDS_BUILDER_FACTORY: JsonArrayRecordsBuilderFactory = JsonArrayRecordsBuilderFactory;

#[cfg(test)]
mod test {
    use crate::batch_records::{JsonArrayRecordsBuilder, RECORDS_BUILDER_FACTORY, RecordsBuilderFactory, JsonArrayRecordsBuilderFactory, RecordsBuilder};

    #[test]
    fn builder_factory() {
        assert_eq!(JsonArrayRecordsBuilder::new(), JsonArrayRecordsBuilderFactory.create_builder());
        assert_eq!(JsonArrayRecordsBuilder::new(), RECORDS_BUILDER_FACTORY.create_builder());
    }

    #[test]
    fn builder_empty() {
        let builder = JsonArrayRecordsBuilder::new();
        assert_eq!(0, builder.len());
        assert_eq!(2, builder.size());
        assert_eq!("[]", builder.build());
    }

    #[test]
    fn builder_one() {
        let mut builder = JsonArrayRecordsBuilder::new();
        builder.add("one");

        assert_eq!(1, builder.len());
        assert_eq!(5, builder.size());
        assert_eq!("[one]", builder.build());
    }

    #[test]
    fn builder_many() {
        let mut builder = JsonArrayRecordsBuilder::new();
        builder.add("one");
        builder.add("two");
        builder.add("three");

        assert_eq!(3, builder.len());
        assert_eq!(15, builder.size());
        assert_eq!("[one,two,three]", builder.build());
    }
}
