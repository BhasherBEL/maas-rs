//! Interpolation of `${VAR}` (environment) and `${file:/path}` (file contents)
//! references inside config strings. Resolved values are secret and never logged.

/// `${file:/path}` reads a file (one trailing newline trimmed); any other token is
/// an environment variable name.
pub fn interpolate(s: &str) -> Result<String, String> {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after
            .find('}')
            .ok_or_else(|| "unterminated '${' in value".to_string())?;
        out.push_str(&resolve_token(&after[..end])?);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

fn resolve_token(token: &str) -> Result<String, String> {
    if let Some(path) = token.strip_prefix("file:") {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("failed to read secret file '{path}': {e}"))?;
        Ok(content.strip_suffix('\n').unwrap_or(&content).to_string())
    } else {
        std::env::var(token).map_err(|_| format!("environment variable '{token}' not set"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literal_passes_through() {
        assert_eq!(
            interpolate("https://example.com/gtfs.zip").unwrap(),
            "https://example.com/gtfs.zip"
        );
    }

    #[test]
    fn env_var_is_substituted() {
        unsafe { std::env::set_var("MAAS_TEST_TOKEN", "secret123") };
        assert_eq!(
            interpolate("Bearer ${MAAS_TEST_TOKEN}").unwrap(),
            "Bearer secret123"
        );
    }

    #[test]
    fn multiple_refs_in_one_string() {
        unsafe { std::env::set_var("MAAS_TEST_A", "a") };
        unsafe { std::env::set_var("MAAS_TEST_B", "b") };
        assert_eq!(interpolate("${MAAS_TEST_A}-${MAAS_TEST_B}").unwrap(), "a-b");
    }

    #[test]
    fn file_ref_is_read_and_trimmed() {
        let dir = std::env::temp_dir();
        let path = dir.join("maas_secret_test.txt");
        std::fs::write(&path, "filesecret\n").unwrap();
        let token = format!("${{file:{}}}", path.display());
        assert_eq!(interpolate(&token).unwrap(), "filesecret");
    }

    #[test]
    fn unknown_env_var_errors() {
        assert!(interpolate("${MAAS_DEFINITELY_UNSET_VAR_XYZ}").is_err());
    }

    #[test]
    fn unterminated_ref_errors() {
        assert!(interpolate("${oops").is_err());
    }
}
