use std::collections::HashMap;

use crate::path::ObjectPath;

enum EffectType {
    Create,
    Read,
    Update,
    Delete,
}

struct Layer {
    objects: HashMap<ObjectPath, Vec<EffectType>>,
}

impl Layer {
    fn new() -> Self {
        Self {
            objects: HashMap::new(),
        }
    }

    fn insert(&mut self, path: ObjectPath, effect: EffectType) {
        let entry = self.objects.entry(path).or_insert(vec![]);
        entry.push(effect);
    }
}

struct Effects {
    layers: Vec<Layer>,
}

impl Effects {
    fn new() -> Self {
        Self {
            layers: vec![Layer::new()],
        }
    }

    fn insert_barrier(&mut self) {
        self.layers.push(Layer::new())
    }

    fn insert(&mut self, path: ObjectPath, effect: EffectType) {
        self.layers.last_mut().unwrap().insert(path, effect);
    }
}
