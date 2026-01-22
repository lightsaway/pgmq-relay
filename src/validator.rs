/// Validator trait for configuration validation
pub trait Validator {
    /// Validate the configuration
    ///
    /// Returns Ok(()) if valid, or Err with a descriptive error message
    fn validate(&self) -> Result<(), String>;
}
