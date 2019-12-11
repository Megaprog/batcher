use std::sync::{Arc, Mutex, MutexGuard};
use std::collections::HashMap;
use std::ops::Deref;

pub struct Inner<'a>(MutexGuard<'a, HashMap<i32, Vec<i32>>>, i32);

impl<'a> Deref for Inner<'a> {
    type Target = Vec<i32>;
    fn deref(&self) -> &Self::Target {
        self.0.get(&self.1).unwrap()
    }
}
pub struct MyStruct {
    data: Arc<Mutex<HashMap<i32, Vec<i32>>>>,
}

impl MyStruct {
    pub fn get_data_for(&self, i: i32) -> ArcHolder {
        ArcHolder(self.data.clone(), i)
    }
//    pub fn get_data_for<'a>(&'a self, i: i32) -> Inner<'a> {
//        let d = self.data.lock().unwrap();
//        Inner(d, i)
//    }
}

pub struct ArcHolder(Arc<Mutex<HashMap<i32, Vec<i32>>>>, i32);

impl/*<'a> AsRef<Inner<'a>> for*/ ArcHolder {
    fn get(&self) -> Inner {
        let d = self.0.lock().unwrap();
        Inner(d, self.1)
    }

//    fn as_ref(&self) -> &Inner<'a> {
//        let d = self.0.lock().unwrap();
//        Inner(d, self.1)
//    }
}

fn main() {
    let mut hm = HashMap::new();
    hm.insert(1, vec![1,2,3]);
    let s = MyStruct {
        data: Arc::new(Mutex::new(hm))
    };

    {
        let v = s.get_data_for(1);
        let i = v.get();
        println!("{:?}", *i);
        let x : Vec<_> = i.iter().map(|x| x * 2).collect();
        println!("{:?}", x); // Just an example to see that it works
    }
}
