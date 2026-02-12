use crate::structures::Ingestor;

#[derive(Debug)]
pub enum SourceLocation {
    Local(String),
    Remote(String),
}

pub fn resolve_path(input: &Ingestor) -> Result<String, String> {
    match input.location()? {
        SourceLocation::Local(path) => Ok(path),
        SourceLocation::Remote(url) => Err(format!("Remote download not yet implemented: {url}")),
    }
}
