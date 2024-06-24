use pingora::{
    ErrorTrait,
    ImmutStr, 
    prelude::*,
};

pub fn perror<S: Into<ImmutStr>, E: Into<Box<dyn ErrorTrait + Send + Sync>>>(
    context: S,
    cause: E,
) -> pingora::BError {
    pingora::Error::because(pingora::ErrorType::InternalError, context, cause)
}

pub fn e_perror<T, S: Into<ImmutStr>, E: Into<Box<dyn ErrorTrait + Send + Sync>>>(
    context: S,
    cause: E,
) -> Result<T> {
    Err(perror(context, cause))
}
