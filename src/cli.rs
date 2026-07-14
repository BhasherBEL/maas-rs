pub fn parse_config_path(args: &[String]) -> Result<String, String> {
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        if let Some(value) = arg.strip_prefix("--config=") {
            return Ok(value.to_string());
        }
        if arg == "--config" {
            return match iter.next() {
                Some(value) => Ok(value.to_string()),
                None => Err("--config requires a path argument".to_string()),
            };
        }
    }
    Ok("config.yaml".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn separate_value() {
        let a = args(&["maas-rs", "--config", "custom.yaml", "--serve"]);
        assert_eq!(parse_config_path(&a).unwrap(), "custom.yaml");
    }

    #[test]
    fn equals_value() {
        let a = args(&["maas-rs", "--config=custom.yaml"]);
        assert_eq!(parse_config_path(&a).unwrap(), "custom.yaml");
    }

    #[test]
    fn default_when_absent() {
        let a = args(&["maas-rs", "--serve"]);
        assert_eq!(parse_config_path(&a).unwrap(), "config.yaml");
    }

    #[test]
    fn missing_value_is_error() {
        let a = args(&["maas-rs", "--config"]);
        assert!(parse_config_path(&a).is_err());
    }
}
