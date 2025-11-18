use crate::cli::OutputFormat;
use crate::error::Result;
use colored::*;
use serde_json::Value;

pub fn format_output(data: &str, format: &OutputFormat) -> Result<String> {
    match format {
        OutputFormat::Json => {
            // Try to parse as JSON and reformat for consistency
            match serde_json::from_str::<Value>(data) {
                Ok(parsed) => {
                    serde_json::to_string_pretty(&parsed)
                        .map_err(crate::error::BraidCliError::SerializationError)
                }
                Err(_) => Ok(data.to_string()), // If not valid JSON, return as-is
            }
        }
        OutputFormat::Pretty => format_pretty(data),
        OutputFormat::Compact => format_compact(data),
    }
}

fn format_pretty(data: &str) -> Result<String> {
    // Try to parse as JSON first
    if let Ok(parsed) = serde_json::from_str::<Value>(data) {
        return format_json_pretty(&parsed);
    }

    // If not JSON, try to format based on content
    if data.trim().is_empty() {
        return Ok("Empty response".to_string());
    }

    // Simple heuristic-based formatting
    if data.chars().count() > 100 {
        Ok(format!("Response:\n{}", indent_text(data, 2)))
    } else {
        Ok(data.to_string())
    }
}

fn format_compact(data: &str) -> Result<String> {
    // Try to parse as JSON and minify
    match serde_json::from_str::<Value>(data) {
        Ok(parsed) => serde_json::to_string(&parsed)
            .map_err(crate::error::BraidCliError::SerializationError),
        Err(_) => {
            // If not JSON, return trimmed version
            Ok(data.trim().to_string())
        }
    }
}

fn format_json_pretty(value: &Value) -> Result<String> {
    match value {
        Value::String(s) => Ok(s.green().to_string()),
        Value::Number(n) => Ok(n.to_string().yellow().to_string()),
        Value::Bool(b) => {
            let colored = if *b { "true".green() } else { "false".red() };
            Ok(colored.to_string())
        }
        Value::Null => Ok("null".dimmed().to_string()),
        Value::Array(arr) => {
            let mut result = String::new();
            result.push_str(&"[".cyan().to_string());
            if !arr.is_empty() {
                result.push('\n');
                for (_i, item) in arr.iter().enumerate() {
                    let item_str = format_json_pretty(item)?;
                    result.push_str(&format!("  {}{},\n", indent_text(&item_str, 2), ""));
                }
                // Remove trailing comma and newline
                result.pop();
                result.pop();
                result.push('\n');
            }
            result.push_str(&"]".cyan().to_string());
            Ok(result)
        }
        Value::Object(obj) => {
            let mut result = String::new();
            result.push_str(&"{".cyan().to_string());
            if !obj.is_empty() {
                result.push('\n');
                for (key, value) in obj.iter() {
                    let value_str = format_json_pretty(value)?;
                    result.push_str(&format!(
                        "  {}{}: {},\n",
                        key.bright_blue().bold(),
                        ":".dimmed(),
                        value_str
                    ));
                }
                // Remove trailing comma and newline
                result.pop();
                result.pop();
                result.push('\n');
            }
            result.push_str(&"}".cyan().to_string());
            Ok(result)
        }
    }
}

fn indent_text(text: &str, spaces: usize) -> String {
    let indent = " ".repeat(spaces);
    text.lines()
        .map(|line| format!("{}{}", if line.trim().is_empty() { "" } else { &indent }, line))
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn print_success(message: &str) {
    println!("{} {}", "✓".green(), message);
}

pub fn print_error(message: &str) {
    eprintln!("{} {}", "✗".red(), message);
}

pub fn print_info(message: &str) {
    println!("{} {}", "ℹ".blue(), message);
}

pub fn print_warning(message: &str) {
    println!("{} {}", "⚠".yellow(), message);
}