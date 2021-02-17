use crate::base::Format;
use crate::state::DatasetState;

trait Validator {
    fn validate(state: DatasetState) -> bool;
}

struct SingleFormatValidator {
    format: Format,
}

impl Validator for SingleFormatValidator {
    fn validate(state: DatasetState) -> bool {
        unimplemented!()
    }
}

struct NoEmptyPartitionsValidator {}

impl Validator for NoEmptyPartitionsValidator {
    fn validate(state: DatasetState) -> bool {
        unimplemented!()
    }
}
